import Axios from "axios";
import {ElasticUtil} from "./ElasticUtil";
import {EmbeddingClient} from "./EmbeddingClient";
import {errorInfo} from "./ErrorUtil";
import * as https from "https";
import fs from "fs";
import * as http from "http";

export type ImportStatus = {
    running: boolean,
    copyDocument: boolean,
    indexMicroChunks: boolean,
    total: number,
    count: number,
    failed: number,
    failedIds: Array<string>,
    startedAt: string,
    finishedAt?: string,
    docsPerMin: number,
    etaMin: number,
    error?: any
}

export class ChunkProcessor {

    private chunkApiUrl: string;

    private elasticsearchHost: string;
    private elasticsearchUser: string;
    private elasticsearchPassword: string;

    private searchUrl: string;

    private agent : https.Agent;

    private noKeepAliveAgent : http.Agent;

    // Chunk files are many small HTTPS downloads from the same host; reusing connections saves a TLS handshake each.
    private chunkAgent : https.Agent;

    // Big chunks and micro chunks are embedded by different models, each served by its own vLLM instance.
    private bigEmbedder: EmbeddingClient;
    private microEmbedder: EmbeddingClient;
    readonly bigIndex: string;
    readonly microIndex: string;
    private readonly bigDims: number;
    private readonly microDims: number;
    private readonly fetchConcurrency: number;

    private elasticUtil: ElasticUtil;

    private importStatus?: ImportStatus;
    private checkedImportIndices = new Set<string>();

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
        this.chunkAgent = new https.Agent({keepAlive: true, maxSockets: parseInt(`${process.env.CHUNK_FETCH_MAX_SOCKETS || 64}`)});
        this.bigEmbedder = new EmbeddingClient(`${process.env.EMBED_BIG_API_URL}`, `${process.env.EMBED_BIG_MODEL}`);
        this.microEmbedder = new EmbeddingClient(`${process.env.EMBED_MICRO_API_URL}`, `${process.env.EMBED_MICRO_MODEL}`);
        this.bigIndex = `${process.env.EMBED_BIG_INDEX}`;
        this.microIndex = `${process.env.EMBED_MICRO_INDEX}`;
        this.bigDims = parseInt(`${process.env.EMBED_BIG_DIMS || 4096}`);
        this.microDims = parseInt(`${process.env.EMBED_MICRO_DIMS || 1024}`);
        this.fetchConcurrency = parseInt(`${process.env.CHUNK_FETCH_CONCURRENCY || 16}`);
    }

    async ensureIndices(): Promise<void> {
        await this.createOrUpdateEmbeddingIndex(this.bigIndex);
        await this.createOrUpdateMicroChunkIndex(this.microIndex);
    }

    // Returns the time spent fetching the chunk metadata, in ms.
    async process(documentId: string): Promise<number> {
        const startTime = Date.now();
        const chunksMeta = await this.fetchChunkMetadata(documentId);
        const metaMs = Date.now() - startTime;
        await this.processChunks(chunksMeta, documentId);
        return metaMs;
    }

    // Indexes big chunks and micro chunks of a document, fetching its chunk metadata only once. Both passes
    // run in parallel: they use different vLLM servers, and the micro chunk downloads overlap with the
    // big chunk embedding. Returns the time spent fetching the chunk metadata, in ms.
    async processDocument(documentId: string): Promise<number> {
        const startTime = Date.now();
        const chunksMeta = await this.fetchChunkMetadata(documentId);
        const metaMs = Date.now() - startTime;
        // Wait for both passes even if one fails, so no work for this document is still running afterwards.
        const results = await Promise.all([
            this.processChunks(chunksMeta, documentId).then(() => ({ok: true, error: undefined as any}), error => ({ok: false, error})),
            this.processMicroChunks(chunksMeta, documentId).then(() => ({ok: true, error: undefined as any}), error => ({ok: false, error}))
        ]);
        const failed = results.find(result => !result.ok);
        if (failed) {
            throw failed.error;
        }
        return metaMs;
    }

    async processChunks(chunksMeta: any, documentId: string): Promise<void> {
        try {
            const startTime = Date.now();
            if (!chunksMeta?.Chunks) {
                console.log(`${documentId}: no chunks`);
            }
            const chunks: Array<any> = (chunksMeta?.Chunks ?? []).map((chunk: any) => ({...chunk, id: ChunkProcessor.normalizeId(chunk.id)}));
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
            console.error(JSON.stringify(errorInfo(error)));
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
            console.error(JSON.stringify(errorInfo(error)));
            throw error;
        }
    }

    // Embeds the micro chunks of all selected big chunks of a document together, so they are sent in
    // as few embedding requests as possible.
    async processMicroChunks(chunksMeta: any, documentId: string, chunkId?: string): Promise<void> {
        try {
            const startTime = Date.now();
            const microChunks: Array<{meta: any, id: string, chunkId: string}> = [];
            for (const chunk of chunksMeta?.Chunks ?? []) {
                const normalizedChunkId = ChunkProcessor.normalizeId(chunk.id);
                if (chunkId && chunkId !== normalizedChunkId) {
                    continue;
                }
                for (const microChunk of chunk.MicroChunks ?? []) {
                    microChunks.push({meta: microChunk, id: ChunkProcessor.normalizeId(microChunk.id), chunkId: normalizedChunkId});
                }
            }
            const existing = await this.elasticUtil.existingIds(this.microIndex, microChunks.map(microChunk => microChunk.id));
            const missing = microChunks.filter(microChunk => !existing.has(microChunk.id));
            if (missing.length === 0) {
                return;
            }
            const texts = await this.mapWithConcurrency(missing, microChunk => this.fetchChunk(microChunk.meta.url, true));
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
            console.error(JSON.stringify(errorInfo(error)));
            throw error;
        }
    }

    private static normalizeId(id: string): string {
        return id.replaceAll("/", "_");
    }

    // Like Promise.all over items.map(fn), but with at most limit (default fetchConcurrency) calls in flight.
    private async mapWithConcurrency<T, R>(items: Array<T>, fn: (item: T) => Promise<R>, limit: number = this.fetchConcurrency): Promise<Array<R>> {
        const results: Array<R> = new Array(items.length);
        let next = 0;
        const worker = async () => {
            while (next < items.length) {
                const i = next++;
                results[i] = await fn(items[i]);
            }
        };
        await Promise.all(Array.from({length: Math.min(limit, items.length)}, worker));
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
            const info = errorInfo(error);
            console.log(`failed to fetch chunk metadata for ${dokid}: ${JSON.stringify(info)}`);
            throw info;
        });
        return responseData;
    }

    // Big chunks are JSON; micro chunks are plain text and must be fetched as text. Axios otherwise
    // JSON-parses any response that looks like JSON, e.g. a micro chunk " 8\n" (a page number) becomes the number 8.
    async fetchChunk(url: string, asText: boolean = false): Promise<any> {
        let responseData;
        const config: any = {
            timeout: 120000,
            httpAgent: this.noKeepAliveAgent,
            httpsAgent: this.chunkAgent
        };
        if (asText) {
            config.responseType = 'text';
            config.transformResponse = [(data: any) => data];
        }
        await this.withRetry(`fetching chunk ${url}`, () => Axios.get(url, config))
            .then((response) => {
                responseData = response.data;
            })
            .catch((error) => {
                const info = errorInfo(error);
                console.log(`failed to fetch chunk ${url}: ${JSON.stringify(info)}`);
                throw info;
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
            console.error(JSON.stringify(errorInfo(error)));
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
            console.error(JSON.stringify(errorInfo(error)));
        }
    }


    // The chunk API uses "0000-00-00" (or partial dates like "2015-00-00") for unknown dates, which
    // Elasticsearch rejects. Those are left out, as DocumentBuilder does for the main documents.
    static validDate(value?: string): string | undefined {
        const match = /^(\d{4})-(\d{2})-(\d{2})/.exec(value ?? '');
        if (!match) {
            return undefined;
        }
        const [year, month, day] = match.slice(1).map(val => parseInt(val, 10));
        const date = new Date(Date.UTC(year, month - 1, day));
        const valid = year > 0 && date.getUTCMonth() === month - 1 && date.getUTCDate() === day;
        return valid ? value : undefined;
    }

    // Parses "dd.mm.yyyy hh:mm:ss" (UTC) as delivered in the chunk's "Zeit UTC" field.
    convertDateString(value?: string): Date | undefined {
        const match = /^(\d{1,2})\.(\d{1,2})\.(\d{4}) (\d{1,2}):(\d{2}):(\d{2})/.exec(value ?? '');
        if (!match) {
            return undefined;
        }
        const [day, month, year, hours, minutes, seconds] = match.slice(1).map(val => parseInt(val, 10));
        return new Date(Date.UTC(year, month - 1, day, hours, minutes, seconds));
    }

    toBigDoc(embedding: Array<number>, documentId: string, chunkData: any): any {
        return {
            embedding: embedding,
            documentId: documentId,
            chunkText: chunkData['Chunktext'],
            scrapyJob: chunkData['ScrapyJob'],
            timeStamp: this.convertDateString(chunkData['Zeit UTC']),
            language: chunkData['Sprache'],
            date: ChunkProcessor.validDate(chunkData['Datum']),
            spider: chunkData['Spider'],
            signature: chunkData['Signatur'],
            pdf: chunkData['PDF'],
            html: chunkData['HTML'],
            num: chunkData['Num'],
            headline: chunkData['Kopfzeile'],
            metadata: chunkData['Meta'],
            abstract: chunkData['Abstract'],
            checkSum: chunkData['Checksum'],
            scrape: ChunkProcessor.validDate(chunkData['Scrapedate']),
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

    isImportRunning(): boolean {
        return this.importStatus?.running ?? false;
    }

    getImportStatus(): ImportStatus | undefined {
        if (this.importStatus === undefined) {
            return undefined;
        }
        return {...this.importStatus, failedIds: this.importStatus.failedIds.slice(0, 100)};
    }

    public async importAll(copyDocument: boolean, indexMicroChunks: boolean): Promise<void> {
        if (this.isImportRunning()) {
            throw new Error('an import is already running');
        }
        const pageSize = parseInt(`${process.env.IMPORT_PAGE_SIZE || 20}`);
        const keepAlive = `${process.env.IMPORT_SCROLL_KEEPALIVE || '60m'}`;
        const concurrency = parseInt(`${process.env.IMPORT_CONCURRENCY || 12}`);
        const status: ImportStatus = {
            running: true, copyDocument, indexMicroChunks, total: 0, count: 0, failed: 0, failedIds: [],
            startedAt: new Date().toISOString(), docsPerMin: 0, etaMin: 0
        };
        this.importStatus = status;
        const startTime = Date.now();
        let scrollId: string | undefined;
        try {
            let response = await this.withRetry('import search', () => Axios.post(
                this.searchUrl + `entscheidsuche.v2*/_search?scroll=${keepAlive}`, {
                    query: {match_all: {}},
                    size: pageSize,
                    // _doc is the cheapest order for scrolling.
                    sort: ["_doc"],
                    // Without this, hits.total is capped at 10000 and the progress total is wrong.
                    track_total_hits: true
                }, {
                    maxContentLength: Infinity,
                    maxBodyLength: Infinity,
                    httpsAgent: this.agent
                }).then(resp => resp.data));
            scrollId = response._scroll_id;
            status.total = response.hits.total.value;
            console.log(`${new Date().toISOString()} import started: ${status.total} documents ` +
                `(copyDocument ${copyDocument}, indexMicroChunks ${indexMicroChunks}, concurrency ${concurrency})`);
            // A pool of workers takes documents from a shared queue, so a slow document never holds up the
            // others. The next scroll page is fetched in the background before the queue runs dry.
            const queue: Array<any> = [...response.hits.hits];
            let exhausted = queue.length === 0;
            let pageError: any;
            let pageFetch: Promise<void> | undefined;
            const fetchNextPage = (): Promise<void> => {
                if (pageFetch === undefined) {
                    pageFetch = (async () => {
                        try {
                            this.updateImportProgress(status, startTime);
                            const page = await this.withRetry('import scroll', () => Axios.post(this.searchUrl + '_search/scroll', {
                                scroll_id: scrollId,
                                scroll: keepAlive
                            }, {
                                maxContentLength: Infinity,
                                maxBodyLength: Infinity,
                                httpsAgent: this.agent
                            }).then(resp => resp.data));
                            scrollId = page._scroll_id;
                            if (page.hits.hits.length === 0) {
                                exhausted = true;
                            } else {
                                queue.push(...page.hits.hits);
                            }
                        } catch (error) {
                            // Stop fetching; the workers finish the documents already queued.
                            pageError = error;
                            exhausted = true;
                        } finally {
                            pageFetch = undefined;
                        }
                    })();
                }
                return pageFetch;
            };
            const worker = async () => {
                while (true) {
                    if (!exhausted && queue.length < pageSize) {
                        const fetching = fetchNextPage();
                        if (queue.length === 0) {
                            await fetching;
                        }
                    }
                    const hit = queue.shift();
                    if (hit === undefined) {
                        if (exhausted) {
                            return;
                        }
                        continue;
                    }
                    await this.importHit(hit, copyDocument, indexMicroChunks, status);
                }
            };
            await Promise.all(Array.from({length: concurrency}, worker));
            this.updateImportProgress(status, startTime);
            if (pageError !== undefined) {
                throw pageError;
            }
        } catch (error) {
            status.error = errorInfo(error);
            console.error(`import aborted after ${status.count}/${status.total} documents: ${JSON.stringify(status.error)}`);
        } finally {
            status.running = false;
            status.finishedAt = new Date().toISOString();
            if (scrollId !== undefined) {
                await this.clearScroll(scrollId);
            }
            const elapsedMin = ((Date.now() - startTime) / 60000).toFixed(1);
            const failedList = status.failedIds.slice(0, 100).join(', ') + (status.failedIds.length > 100 ? ', ...' : '');
            console.log(`${new Date().toISOString()} import finished: ${status.count}/${status.total} documents processed, ` +
                `${status.failed} failed, ${elapsedMin} min` + (status.failed > 0 ? `. Failed: ${failedList}` : ''));
        }
    }

    private async importHit(hit: any, copyDocument: boolean, indexMicroChunks: boolean, status: ImportStatus): Promise<void> {
        const docStart = Date.now();
        try {
            if (copyDocument) await this.processImportHits(hit)
            const metaMs = indexMicroChunks ? await this.processDocument(hit._id) : await this.process(hit._id);
            status.count++;
            console.log(`import ${status.count}/${status.total}: ${hit._id} done in ${Date.now() - docStart} ms (meta ${metaMs} ms)`);
        }
        catch (err) {
            status.count++;
            status.failed++;
            status.failedIds.push(hit._id);
            console.error(`import ${status.count}/${status.total}: ${hit._id} failed after ${Date.now() - docStart} ms: ${JSON.stringify(errorInfo(err))}`);
        }
    }

    private updateImportProgress(status: ImportStatus, startTime: number): void {
        const elapsedMs = Date.now() - startTime;
        status.docsPerMin = status.count / (elapsedMs / 60000);
        status.etaMin = status.docsPerMin > 0 ? (status.total - status.count) / status.docsPerMin : 0;
        const percent = status.total > 0 ? (100 * status.count / status.total).toFixed(1) : '100.0';
        console.log(`${new Date().toISOString()} import progress: ${status.count}/${status.total} (${percent}%), ` +
            `${status.failed} failed, ${status.docsPerMin.toFixed(1)} docs/min, ` +
            `elapsed ${(elapsedMs / 60000).toFixed(1)} min, ETA ${status.etaMin.toFixed(1)} min`);
    }

    // Retries transient failures (connection errors, 5xx) so a single hiccup does not end a long import.
    private async withRetry<T>(what: string, fn: () => Promise<T>, maxRetries: number = 3): Promise<T> {
        for (let attempt = 0; ; attempt++) {
            try {
                return await fn();
            } catch (err: any) {
                const status = err.response?.status;
                const retryable = status === undefined || status >= 500;
                if (!retryable || attempt >= maxRetries) {
                    throw err;
                }
                const delayMs = 2000 * Math.pow(2, attempt);
                console.log(`${what} failed (${err.message}), retrying in ${delayMs} ms`);
                await new Promise(resolve => setTimeout(resolve, delayMs));
            }
        }
    }

    private async clearScroll(scrollId: string): Promise<void> {
        await Axios.delete(this.searchUrl + '_search/scroll', {
            data: {scroll_id: scrollId},
            httpsAgent: this.agent
        }).catch(err => console.log(`failed to clear import scroll: ${err.message}`));
    }

    //Importing data to Test-ElasticSearch
    private async processImportHits(hit: any){
        if (!this.checkedImportIndices.has(hit._index)) {
            if (!await this.elasticUtil.existsIndex(hit._index)){
                await this.createDocIndex(hit._index);
            }
            this.checkedImportIndices.add(hit._index);
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