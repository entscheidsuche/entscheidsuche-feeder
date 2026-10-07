import Axios from "axios";
import {ElasticUtil} from "./ElasticUtil";
import {EmbeddingClient} from "./EmbeddingClient";
import * as https from "https";
import fs from "fs";
import * as http from "http";

export class ChunkProcessor {

    private chunkApiUrl: string;

    private elasticsearchHost: string;
    private elasticsearchUser: string;
    private elasticsearchPassword: string;

    private searchUrl: string;

    private agent : https.Agent;

    private noKeepAliveAgent : http.Agent;

    // Big chunks and micro chunks are embedded by different models, each served by its own vLLM instance.
    private bigEmbedder: EmbeddingClient;
    private microEmbedder: EmbeddingClient;
    readonly bigIndex: string;
    readonly microIndex: string;
    private readonly bigDims: number;
    private readonly microDims: number;
    private readonly fetchConcurrency: number;

    private elasticUtil: ElasticUtil;

    constructor() {
        this.chunkApiUrl = `${process.env.CHUNK_API_URL}`;
        this.elasticsearchHost = `${process.env.ELASTICSEARCH_HOST}`;
        this.elasticsearchUser = `${process.env.ELASTICSEARCH_USER}`;
        this.elasticsearchPassword = `${process.env.ELASTICSEARCH_PASSWORD}`;
        this.elasticUtil = new ElasticUtil();
        this.searchUrl = `${process.env.IMPORT_SEARCH_URL}`;
        this.agent = new https.Agent({
            ca: fs.readFileSync(`${process.env.ELASTICSEARCH_CERT_PATH}`),
            rejectUnauthorized: false
        });
        this.noKeepAliveAgent = new http.Agent({keepAlive: false});
        this.bigEmbedder = new EmbeddingClient(`${process.env.EMBED_BIG_API_URL}`, `${process.env.EMBED_BIG_MODEL}`);
        this.microEmbedder = new EmbeddingClient(`${process.env.EMBED_MICRO_API_URL}`, `${process.env.EMBED_MICRO_MODEL}`);
        this.bigIndex = `${process.env.EMBED_BIG_INDEX}`;
        this.microIndex = `${process.env.EMBED_MICRO_INDEX}`;
        this.bigDims = parseInt(`${process.env.EMBED_BIG_DIMS || 4096}`);
        this.microDims = parseInt(`${process.env.EMBED_MICRO_DIMS || 1024}`);
        this.fetchConcurrency = parseInt(`${process.env.CHUNK_FETCH_CONCURRENCY || 8}`);
    }

    async ensureIndices(): Promise<void> {
        await this.createOrUpdateEmbeddingIndex(this.bigIndex);
        await this.createOrUpdateMicroChunkIndex(this.microIndex);
    }

    async process(documentId: string): Promise<void> {
        await this.processChunks(await this.fetchChunkMetadata(documentId), documentId);
    }

    // Indexes big chunks and micro chunks of a document, fetching its chunk metadata only once.
    async processDocument(documentId: string): Promise<void> {
        const chunksMeta = await this.fetchChunkMetadata(documentId);
        await this.processChunks(chunksMeta, documentId);
        await this.processMicroChunks(chunksMeta, documentId);
    }

    async processChunks(chunksMeta: any, documentId: string): Promise<void> {
        try {
            const startTime = Date.now();
            const chunks: Array<any> = chunksMeta.Chunks.map((chunk: any) => ({...chunk, id: ChunkProcessor.normalizeId(chunk.id)}));
            const existing = await this.elasticUtil.existingIds(this.bigIndex, chunks.map(chunk => chunk.id));
            const missing = chunks.filter(chunk => !existing.has(chunk.id));
            if (missing.length === 0) {
                return;
            }
            const fetched = await this.mapWithConcurrency(missing, chunk => this.fetchChunk(chunk.url));
            const toEmbed = missing
                .map((chunk, i) => ({chunk, chunkData: fetched[i]}))
                .filter(entry => entry.chunkData && entry.chunkData.Chunktext);
            const endTimeFetch = Date.now();
            const embeddings = await this.bigEmbedder.embed(toEmbed.map(entry => entry.chunkData.Chunktext));
            const endTimeEmbed = Date.now();
            await this.elasticUtil.bulkIndex(this.bigIndex, toEmbed.map((entry, i) => ({
                id: entry.chunk.id,
                body: this.toBigDoc(embeddings[i], documentId, entry.chunkData)
            })));
            console.log(`${documentId}: ${toEmbed.length} chunks, fetch ${endTimeFetch - startTime} ms, ` +
                `embed ${endTimeEmbed - endTimeFetch} ms, index ${Date.now() - endTimeEmbed} ms`);
        }
        catch (error) {
            console.error(error);
            throw error;
        }
    }

    async indexMicroChunks(documentId: string, chunkId?: string): Promise<void> {
        try {
            const startTimeFetch = Date.now();
            const chunksMeta = await this.fetchChunkMetadata(documentId);
            const endTimeFetch = Date.now();
            console.log(`fetched chunkMeta in ${endTimeFetch - startTimeFetch} ms`);
            await this.processMicroChunks(chunksMeta, documentId, chunkId)
        }
        catch (error) {
            console.error(error);
            throw error;
        }
    }

    // Embeds the micro chunks of all selected big chunks of a document together, so they are sent in
    // as few embedding requests as possible.
    async processMicroChunks(chunksMeta: any, documentId: string, chunkId?: string): Promise<void> {
        try {
            const startTime = Date.now();
            const microChunks: Array<{meta: any, id: string, chunkId: string}> = [];
            for (const chunk of chunksMeta.Chunks) {
                const normalizedChunkId = ChunkProcessor.normalizeId(chunk.id);
                if (chunkId && chunkId !== normalizedChunkId) {
                    continue;
                }
                for (const microChunk of chunk.MicroChunks) {
                    microChunks.push({meta: microChunk, id: ChunkProcessor.normalizeId(microChunk.id), chunkId: normalizedChunkId});
                }
            }
            const existing = await this.elasticUtil.existingIds(this.microIndex, microChunks.map(microChunk => microChunk.id));
            const missing = microChunks.filter(microChunk => !existing.has(microChunk.id));
            if (missing.length === 0) {
                return;
            }
            const texts = await this.mapWithConcurrency(missing, microChunk => this.fetchChunk(microChunk.meta.url));
            const toEmbed = missing
                .map((microChunk, i) => ({microChunk, chunkText: texts[i]}))
                .filter(entry => entry.chunkText);
            const endTimeFetch = Date.now();
            const embeddings = await this.microEmbedder.embed(toEmbed.map(entry => entry.chunkText));
            const endTimeEmbed = Date.now();
            await this.elasticUtil.bulkIndex(this.microIndex, toEmbed.map((entry, i) => ({
                id: entry.microChunk.id,
                body: this.toMicroDoc(entry.microChunk.meta, embeddings[i], documentId, entry.chunkText, entry.microChunk.chunkId)
            })));
            console.log(`${documentId}: ${toEmbed.length} micro chunks, fetch ${endTimeFetch - startTime} ms, ` +
                `embed ${endTimeEmbed - endTimeFetch} ms, index ${Date.now() - endTimeEmbed} ms`);
        }
        catch (error) {
            console.error(error);
            throw error;
        }
    }

    private static normalizeId(id: string): string {
        return id.replaceAll("/", "_");
    }

    // Like Promise.all over items.map(fn), but with at most fetchConcurrency calls in flight.
    private async mapWithConcurrency<T, R>(items: Array<T>, fn: (item: T) => Promise<R>): Promise<Array<R>> {
        const results: Array<R> = new Array(items.length);
        let next = 0;
        const worker = async () => {
            while (next < items.length) {
                const i = next++;
                results[i] = await fn(items[i]);
            }
        };
        await Promise.all(Array.from({length: Math.min(this.fetchConcurrency, items.length)}, worker));
        return results;
    }

    async fetchChunkMetadata(dokid: string): Promise<any> {
        let responseData;
        await Axios.get(this.chunkApiUrl, {
            params: {
               dokid
            },
            timeout: 120000,
            httpAgent: this.noKeepAliveAgent
        }).then((response) => {
            responseData = response.data;
        }).catch((error) => {
            console.log(error);
            throw(error);
        });
        return responseData;
    }

    async fetchChunk(url: string): Promise<any> {
        let responseData;
        await Axios.get(url, {
            timeout: 120000,
            httpAgent: this.noKeepAliveAgent
        })
            .then((response) => {
                responseData = response.data;
            })
            .catch((error) => {
                console.log(error);
                throw(error);
            });
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
                        "dims": this.bigDims,
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
                        "dims": this.microDims,
                        "index": true,
                        "similarity": "cosine",
                    },
                    "chunkText": {
                        "type": "text",
                    },
                    "offset": {
                        "type": "integer",
                    },
                    "len": {
                        "type": "integer",
                    },
                    "chunkId": {
                        "type": "keyword"
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

    toBigDoc(embedding: Array<number>, documentId: string, chunkData: any): any {
        return {
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
        };
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

    toMicroDoc(microChunkMeta: any, embedding: Array<number>, documentId: string, chunkText: string, chunkId: string): any {
        return {
            embedding: embedding,
            documentId: documentId,
            chunkText: chunkText,
            offset: microChunkMeta.offset,
            len: microChunkMeta.len,
            chunkId: chunkId
        };
    }

    public async importAll(copyDocument: boolean, indexMicroChunks: boolean): Promise<any> {
        let scrollSearch = {
            "query": {
                "query_string": {
                    "query": "*"
                }
            },
            "size": 50,
            "sort": [],
            // Without this, hits.total is capped at 10000 and the progress total is wrong.
            "track_total_hits": true
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
        let failed = 0;
        const startTime = Date.now();
        console.log(`${new Date().toISOString()} import started: ${totalCount} documents ` +
            `(copyDocument ${copyDocument}, indexMicroChunks ${indexMicroChunks})`);
        while(response.hits.hits.length > 0) {
            try{
                for (const hit of response.hits.hits) {
                    const docStart = Date.now();
                    try {
                        if (copyDocument) await this.processImportHits(hit)
                        if (indexMicroChunks) await this.processDocument(hit._id)
                        else await this.process(hit._id)
                        console.log(`import ${count + 1}/${totalCount}: ${hit._id} done in ${Date.now() - docStart} ms`);
                    }
                    catch(err) {
                        failed++;
                        console.error(`import ${count + 1}/${totalCount}: ${hit._id} failed after ${Date.now() - docStart} ms:`, err)
                    }
                    count++;
                }
                this.logImportProgress(count, totalCount, failed, startTime);
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
                console.error(`import aborted after ${count}/${totalCount} documents:`, error);
                break
            }

        }
        const elapsedMin = ((Date.now() - startTime) / 60000).toFixed(1);
        console.log(`${new Date().toISOString()} import finished: ${count}/${totalCount} documents processed, ` +
            `${failed} failed, ${elapsedMin} min`);
    }

    private logImportProgress(count: number, totalCount: number, failed: number, startTime: number): void {
        const elapsedMs = Date.now() - startTime;
        const docsPerMin = count / (elapsedMs / 60000);
        const remainingMin = docsPerMin > 0 ? (totalCount - count) / docsPerMin : 0;
        const percent = totalCount > 0 ? (100 * count / totalCount).toFixed(1) : '100.0';
        console.log(`${new Date().toISOString()} import progress: ${count}/${totalCount} (${percent}%), ` +
            `${failed} failed, ${docsPerMin.toFixed(1)} docs/min, ` +
            `elapsed ${(elapsedMs / 60000).toFixed(1)} min, ETA ${remainingMin.toFixed(1)} min`);
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