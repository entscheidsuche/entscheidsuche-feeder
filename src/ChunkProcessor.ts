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

    private embedding_index;


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
        this.embedding_index = "embeddings_" + this.llmModel + "_2";
    }

    async process(documentId: string): Promise<void> {
        try {
            const startTimeFetch = Date.now();
            const chunksMeta = await this.fetchChunkMetadata(documentId);
            const endTimeFetch = Date.now();
            const index = this.embedding_index;
            console.log(`fetched chunkMeta in ${endTimeFetch - startTimeFetch} ms`);
            for (const chunk of chunksMeta.Chunks) {
                chunk.id = chunk.id.replaceAll("/", "_");
                if(await this.elasticUtil.existsDocument(chunk.id, index)) {
                    continue;
                }
                console.log(`${new Date().toISOString()} processing chunk ${chunk.id}`);
                const startTimeFetchChunk = Date.now();
                const chunkData = await this.fetchChunk(chunk.url);
                const endTimeFetchChunk = Date.now();
                console.log(`fetched chunk in ${endTimeFetchChunk - startTimeFetchChunk} ms`);
                if (chunkData) {
                    const embeddingResponse = await this.getEmbedding(chunkData.Chunktext);
                    if (embeddingResponse) {
                        const embedding = embeddingResponse.data[0].embedding;
                        const endTimeEmbed = Date.now();
                        console.log(`got embedding in ${endTimeEmbed - endTimeFetch} ms`);
                        if (embedding) {
                            await this.upsert(chunk.id, embedding, documentId, chunkData);
                            const endTimeUpsert = Date.now();
                            console.log(`upserted chunk in ${endTimeUpsert - endTimeEmbed} ms`);
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

    async indexMicroChunks(documentId: string): Promise<void> {
        try {
            const startTimeFetch = Date.now();
            const chunksMeta = await this.fetchChunkMetadata(documentId);
            const endTimeFetch = Date.now();
            console.log(`fetched chunkMeta in ${endTimeFetch - startTimeFetch} ms`);
            await this.processMicroChunks(chunksMeta, documentId)
        }
        catch (error) {
            console.error(error);
            throw error;
        }
    }

    async processMicroChunks(chunksMeta: any, documentId: string): Promise<void> {
        try {
            const index = "embeddings_" + this.llmModel + "_micro";
            for (const microChunk of chunksMeta.MicroChunks) {
                const microChunkId = microChunk.id.replaceAll("/", "_");
                if(await this.elasticUtil.existsDocument(microChunkId, index)) {
                    continue;
                }
                console.log(`${new Date().toISOString()} processing chunk ${microChunkId}`);
                const startTimeFetch = Date.now();
                const chunkText = await this.fetchChunk(microChunk.url);
                const endTimeFetch = Date.now();
                console.log(`fetched chunk in ${endTimeFetch - startTimeFetch} ms`);
                if (chunkText) {
                    const embeddingResponse = await this.getEmbedding(chunkText);
                    if (embeddingResponse) {
                        const embedding = embeddingResponse.data[0].embedding;
                        const endTimeEmbed = Date.now();
                        console.log(`got embedding in ${endTimeEmbed - endTimeFetch} ms`);
                        if (embedding) {
                            await this.upsertMicroChunk(microChunk, embedding, documentId, chunkText);
                            const endTimeUpsert = Date.now();
                            console.log(`upserted chunk in ${endTimeUpsert - endTimeEmbed} ms`);
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
            },
            timeout: 120000,
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

    async createOrUpdateEmbeddingIndex(name: string): Promise<any> {
        try {
            const properties = {
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
                    },
                    "scrapyJob" : {
                        "type": "keyword",
                    },
                    "timeStamp": {
                        "type": "date"
                    },
                    "language" : {
                        "type": "keyword",
                    },
                    "date" : {
                        "type": "date",
                    },
                    "scrapeDate" : {
                        "type": "date",
                    },
                    "spider": {
                        "type": "keyword",
                    },
                    "signature" : {
                        "type": "keyword",
                    },
                    "pdf" : {
                        "type": "object",
                    },
                    "html" : {
                        "type": "object",
                    },
                    "num": {
                        "type": "keyword",
                    },
                    "headline": {
                        "type": "object",
                    },
                    "metadata" : {
                        "type": "object",
                    },
                    "abstract" : {
                        "type": "object",
                    },
                    "checkSum" : {
                        "type": "text",
                    },
                    "hierarchy" : {
                        "type": "keyword",
                    }
                }
            }
            const mapping = {
                "mappings": properties,
            }
            if (!await this.elasticUtil.existsIndex(name))
                return await this.elasticUtil.createIndex(name, mapping);
            else {
                return await this.elasticUtil.updateIndex(name, properties);
            }
        }
        catch (error) {
            console.error(error);
        }

    }


    async createOrUpdateMicroChunkIndex(name: string): Promise<any> {
        try {
            const properties = {
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
                    },
                    "offset": {
                        "type": "integer",
                    },
                    "length": {
                        "type": "integer",
                    }

                }
            }
            const mapping = {
                "mappings": properties,
            }
            if (!await this.elasticUtil.existsIndex(name))
                return await this.elasticUtil.createIndex(name, mapping);
            else {
                return await this.elasticUtil.updateIndex(name, properties);
            }
        }
        catch (error) {
            console.error(error);
        }
    }


    convertDateString(value: string): Date {
        const dateVals = value.split(' ')[0].split('.').map(val => parseInt(val, 10));
        const timeVals = value.split(' ')[1].split(':').map(val => parseInt(val, 10));

        return new Date(dateVals[2], dateVals[1], dateVals[0], timeVals[0], timeVals[1], timeVals[2]);

    }

    async upsert(id: string, embedding: Array<number>, documentId: string, chunkData: any): Promise<void> {
        const index = this.embedding_index;

        if (!await this.elasticUtil.existsIndex(index)) {
            await this.createOrUpdateEmbeddingIndex(index);
        }

        const data ={
            embedding: embedding,
            documentId: documentId,
            chunkText: chunkData['Chunktext'],
            scrapyJob: chunkData['ScrapyJob'],
            timeStamp: this.convertDateString(chunkData['Zeit UTC']),
            language: chunkData['Sprache'],
            date: chunkData['Datum'],
            spider: chunkData['Spider'],
            signature: chunkData['Signatur'],
            pdf: chunkData['PDF'],
            html: chunkData['HTML'],
            num: chunkData['Num'],
            headline: chunkData['Kopfzeile'],
            metadata: chunkData['Meta'],
            abstract: chunkData['Abstract'],
            checkSum: chunkData['Checksum'],
            scrape: chunkData['Scrapedate'],
            hierarchy: this.buildHierarchy(chunkData['Signatur']),
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

    buildHierarchy(signature: string): string[] {
        const parts = signature.split("_");
        let result: string[] = [];
        for (let i = 0; i < parts.length; i++) {
            let part = parts[0];
            for (let j = 1; j <= i; j++) {
                part += "_" + parts[j];
            }
            result.push(part);
        }
        return result;
    }

    async upsertMicroChunk(microChunkMeta: any, embedding: Array<number>, documentId: string, chunkText: string): Promise<void> {
        const index = "embeddings_" + this.llmModel + "_micro";

        if (!await this.elasticUtil.existsIndex(index)) {
            await this.createOrUpdateMicroChunkIndex(index);
        }

        const data ={
            embedding: embedding,
            documentId: documentId,
            chunkText: chunkText,
            offset: microChunkMeta.offset,
            len: microChunkMeta.len,
        }
        return Axios.put(`${this.elasticsearchHost}/${index}/_doc/${microChunkMeta.id}`, data, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            auth: {
                username: this.elasticsearchUser,
                password: this.elasticsearchPassword
            },
            httpsAgent: this.agent
        }).then(() => {
            console.log(`inserting document ${microChunkMeta.id}`)
        }).catch(err => {
            if (err.response && err.response.data && err.response.data.error) {
                throw { document: microChunkMeta.id, response: err.response.data.error }
            } else {
                throw { document: microChunkMeta.id, response: err };
            }
        });


    }

    public async importAll(copyDocument: boolean, indexMicroChunks: boolean): Promise<any> {
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
                    console.log(hit._id);
                    try {
                        if (copyDocument) await this.processImportHits(hit)
                        await this.process(hit._id)
                        if (indexMicroChunks) await this.indexMicroChunks(hit._id)
                    }
                    catch(err) {
                        console.error(err)
                    }
                    count++;
                }
                if (count>totalCount) break;
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
                break
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