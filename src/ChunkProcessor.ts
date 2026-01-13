import Axios from "axios";
import {ElasticUtil} from "./ElasticUtil";
import * as https from "https";
import fs from "fs";

export class ChunkProcessor {

    private chunkApiUrl: string;
    private llmApiUrl: string;
    private llmModel: string;

    private elasticsearchHost: string;
    private elasticsearchUser: string;
    private elasticsearchPassword: string;

    private searchUrl: string;

    private agent : https.Agent;


    private elasticUtil: ElasticUtil;

    constructor() {
        this.chunkApiUrl = `${process.env.CHUNK_API_URL}`;
        this.llmApiUrl = `${process.env.LLM_API_URL}`;
        this.llmModel = `${process.env.LLM_MODEL}`;
        this.elasticsearchHost = `${process.env.ELASTICSEARCH_HOST}`;
        this.elasticsearchUser = `${process.env.ELASTICSEARCH_USER}`;
        this.elasticsearchPassword = `${process.env.ELASTICSEARCH_PASSWORD}`;
        this.elasticUtil = new ElasticUtil();
        this.searchUrl = `${process.env.IMPORT_SEARCH_URL}`;
        this.agent = new https.Agent({
            ca: fs.readFileSync(`${process.env.ELASTICSEARCH_CERT_PATH}`),
            rejectUnauthorized: false
        });
    }

    async process(documentId: string): Promise<void> {
        try {
            const chunksMeta = await this.fetchChunkMetadata(documentId);
            for (const chunk of chunksMeta.Chunks) {
                console.log(`${new Date().toISOString()} processing chunk ${chunk.id}`);
                const chunkData = await this.fetchChunk(chunk.url);
                if (chunkData) {
                    const embeddingResponse = await this.getEmbedding(chunkData.Chunktext);
                    if (embeddingResponse) {
                        const embedding = embeddingResponse.data[0].embedding;
                        if (embedding) {
                            await this.upsert(chunk.id, embedding, documentId, chunkData.Chunktext);
                        }
                    }
                }
            }
        }
        catch (error) {
            console.error(error);
            throw error;
        }
    }

    async fetchChunkMetadata(dokid: string): Promise<any> {
        let responseData;
        await Axios.get(this.chunkApiUrl, {
            params: {
               dokid
            }
        }).then((response) => {
            responseData = response.data;
        }).catch((error) => {
            console.log(error);
        });
        return responseData;
    }

    async fetchChunk(url: string): Promise<any> {
        let responseData;
        await Axios.get(url)
            .then((response) => {
                responseData = response.data;
            })
            .catch((error) => {
                console.log(error);
            });
        return responseData;

    }

    async getEmbedding(chunkText: string): Promise<any> {
        let responseData;
        let chunkData: {input: string; model: string, encoding_format: string} = {input: chunkText, encoding_format: "float", model: this.llmModel};

        await Axios.post(this.llmApiUrl + '/v1/embeddings', chunkData, {})
            .then((response) => {
                responseData = response.data;
            })
            .catch((error) => {
                console.log(error);
            })
        return responseData;
    }

    async createEmbeddingIndex(name: string): Promise<any> {
        try {
            const mapping = {
                "mappings": {
                    "properties": {
                        "documentId": {
                            "type": "keyword"
                        },
                        "embedding": {
                            "type": "dense_vector",
                            "dims": 4096,
                            "index": true,
                            "similarity": "cosine",
                        },
                        "chunkText": {
                            "type": "text",
                        }
                    }
                }
            }

            return await this.elasticUtil.createIndex(name, mapping);
        }
        catch (error) {
            console.error(error);
        }

    }

    async upsert(id: string, embedding: Array<number>, documentId: string, chunkText: string): Promise<void> {
        const index = "embeddings_" + this.llmModel;

        if (!await this.elasticUtil.existsIndex(index)) {
            await this.createEmbeddingIndex(index);
        }

        const data ={
            embedding: embedding,
            documentId: documentId,
            chunkText: chunkText,
        }
        return Axios.put(`${this.elasticsearchHost}/${index}/_doc/${id}`, data, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            auth: {
                username: this.elasticsearchUser,
                password: this.elasticsearchPassword
            },
            httpsAgent: this.agent
        }).then(() => {
            console.log(`inserting document ${id}`)
        }).catch(err => {
            if (err.response && err.response.data && err.response.data.error) {
                throw { document: id, response: err.response.data.error }
            } else {
                throw { document: id, response: err };
            }
        });


    }

    public async importAll(): Promise<any> {
        let scrollSearch = {
            "query": {
                "query_string": {
                    "query": "*"
                }
            },
            "size": 50,
            "sort": []
        }

        let response = await Axios.post(this.searchUrl + 'entscheidsuche.v2*/_search?scroll=30m', scrollSearch, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            httpsAgent: this.agent
        }).then(resp => {
            return resp.data;
        });
        let scrollId = response._scroll_id
        const totalCount = response.hits.total.value
        let count = 0;
        while(response.hits.hits.length > 0) {
            try{
                for (const hit of response.hits.hits) {
                    console.log(`${count}/${totalCount}`);
                    try {
                        await this.processImportHits(hit)
                        await this.process(hit._id)
                    }
                    catch(error) {
                        console.error(error);
                    }
                    count++;
                }
                response = await Axios.post(this.searchUrl + '_search/scroll', {
                    scroll_id: scrollId,
                    scroll: "30m"
                }, {
                    maxContentLength: Infinity,
                    maxBodyLength: Infinity,
                    httpsAgent: this.agent
                    }
                ).then(resp => {
                    return resp.data;
                })
                scrollId = response._scroll_id
            }
            catch(error) {
                console.error(error);
            }

        }



    }

    //Importing data to Test-ElasticSearch
    private async processImportHits(hit: any){
        if (!await this.elasticUtil.existsIndex(hit._index)){
            await this.createDocIndex(hit._index);
        }
        const resp = await Axios.put(
            `${this.elasticsearchHost}/${hit._index}/_doc/${hit._id}`,
            hit._source,
            {
                httpsAgent: this.agent,
                auth: {
                    username: this.elasticsearchUser,
                    password: this.elasticsearchPassword
                }
            }
        );
        return resp.data;
    }

    private async createDocIndex(name: string) {
        return await this.elasticUtil.createIndex(name,{
            "mappings": {
                "dynamic": "true",
                "dynamic_date_formats": [
                    "strict_date_optional_time",
                    "yyyy/MM/dd HH:mm:ss Z||yyyy/MM/dd Z"
                ],
                "dynamic_templates": [],
                "date_detection": true,
                "numeric_detection": false,
                "properties": {
                    "abstract": {
                        "properties": {
                            "de": {
                                "type": "text"
                            },
                            "fr": {
                                "type": "text"
                            },
                            "it": {
                                "type": "text"
                            }
                        }
                    },
                    "attachment": {
                        "properties": {
                            "author": {
                                "type": "text"
                            },
                            "content": {
                                "type": "text",
                                "store": true
                            },
                            "content_length": {
                                "type": "long"
                            },
                            "content_type": {
                                "type": "keyword"
                            },
                            "content_url": {
                                "type": "keyword"
                            },
                            "date": {
                                "type": "date"
                            },
                            "language": {
                                "type": "keyword"
                            },
                            "source": {
                                "type": "keyword"
                            },
                            "title": {
                                "type": "text"
                            }
                        }
                    },
                    "canton": {
                        "type": "keyword"
                    },
                    "date": {
                        "type": "date"
                    },
                    "hierarchy": {
                        "type": "keyword"
                    },
                    "id": {
                        "type": "keyword"
                    },
                    "meta": {
                        "properties": {
                            "de": {
                                "type": "text"
                            },
                            "fr": {
                                "type": "text"
                            },
                            "it": {
                                "type": "text"
                            }
                        }
                    },
                    "reference": {
                        "type": "keyword"
                    },
                    "scrapedate": {
                        "type": "date"
                    },
                    "source": {
                        "type": "keyword"
                    },
                    "title": {
                        "dynamic": "true",
                        "properties": {
                            "de": {
                                "type": "text"
                            },
                            "fr": {
                                "type": "text"
                            },
                            "it": {
                                "type": "text"
                            }
                        }
                    }
                }
            }
        })
    }

}