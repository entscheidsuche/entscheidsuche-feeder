import Axios from "axios";
import { ElasticUtil } from "./ElasticUtil";
import https from "https";
import fs from "fs";

export enum ChunkQueueStatus {
    PENDING = "pending",
    PROCESSING = "processing",
    FAILED = "failed"
}

export interface ChunkQueueItem {
    id: string;
    documentId: string;
    attempts: number;
    seqNo: number;
    primaryTerm: number;
}

export class ChunkQueue {

    private elasticsearchHost: string;
    private elasticsearchUser: string;
    private elasticsearchPassword: string;
    private readonly queueIndex: string;
    private agent: https.Agent;
    private elasticUtil: ElasticUtil;
    private indexReady: Promise<void> | undefined;

    constructor() {
        this.elasticsearchHost = `${process.env.ELASTICSEARCH_HOST}`;
        this.elasticsearchUser = `${process.env.ELASTICSEARCH_USER}`;
        this.elasticsearchPassword = `${process.env.ELASTICSEARCH_PASSWORD}`;
        this.queueIndex = `${process.env.CHUNK_QUEUE_INDEX || 'chunk_queue'}`;
        this.elasticUtil = new ElasticUtil();
        this.agent = new https.Agent({
            ca: fs.readFileSync(`${process.env.ELASTICSEARCH_CERT_PATH}`),
            rejectUnauthorized: false
        });
    }

    async ensureIndex(): Promise<void> {
        if (this.indexReady === undefined) {
            this.indexReady = (async () => {
                if (!await this.elasticUtil.existsIndex(this.queueIndex)) {
                    await this.elasticUtil.createIndex(this.queueIndex, {
                        mappings: {
                            properties: {
                                documentId: { type: "keyword" },
                                status: { type: "keyword" },
                                attempts: { type: "integer" },
                                error: { type: "text" },
                                createdAt: { type: "date" },
                                updatedAt: { type: "date" }
                            }
                        }
                    });
                }
            })().catch(err => {
                this.indexReady = undefined;
                throw err;
            });
        }
        return this.indexReady;
    }

    async enqueue(documentId: string): Promise<void> {
        await this.ensureIndex();
        const now = new Date().toISOString();
        const id = encodeURIComponent(documentId);
        return Axios.put(`${this.elasticsearchHost}/${this.queueIndex}/_doc/${id}`, {
            documentId: documentId,
            status: ChunkQueueStatus.PENDING,
            attempts: 0,
            error: null,
            createdAt: now,
            updatedAt: now
        }, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            auth: {
                username: this.elasticsearchUser,
                password: this.elasticsearchPassword
            },
            httpsAgent: this.agent
        }).then(() => {
            console.log(`enqueued chunk job for document ${documentId}`);
        }).catch(err => {
            if (err.response && err.response.data && err.response.data.error) {
                throw { document: documentId, response: err.response.data.error };
            } else {
                throw { document: documentId, response: err };
            }
        });
    }

    async claimBatch(batchSize: number): Promise<Array<ChunkQueueItem>> {
        await this.ensureIndex();
        const query = {
            size: batchSize,
            query: { term: { status: ChunkQueueStatus.PENDING } },
            sort: [{ createdAt: "asc" }],
            seq_no_primary_term: true
        };
        const resp = await Axios.post(`${this.elasticsearchHost}/${this.queueIndex}/_search`, query, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            auth: {
                username: this.elasticsearchUser,
                password: this.elasticsearchPassword
            },
            httpsAgent: this.agent
        });

        const hits: Array<any> = resp.data?.hits?.hits ?? [];
        const claimed: Array<ChunkQueueItem> = [];
        for (const hit of hits) {
            const item: ChunkQueueItem = {
                id: hit._id,
                documentId: hit._source.documentId,
                attempts: hit._source.attempts ?? 0,
                seqNo: hit._seq_no,
                primaryTerm: hit._primary_term
            };
            const result = await this.markProcessing(item);
            if (result !== undefined) {
                claimed.push(result);
            }
        }
        return claimed;
    }

    private async markProcessing(item: ChunkQueueItem): Promise<ChunkQueueItem | undefined> {
        try {
            const resp = await Axios.post(
                `${this.elasticsearchHost}/${this.queueIndex}/_update/${encodeURIComponent(item.id)}` +
                `?if_seq_no=${item.seqNo}&if_primary_term=${item.primaryTerm}`,
                {
                    doc: {
                        status: ChunkQueueStatus.PROCESSING,
                        updatedAt: new Date().toISOString()
                    }
                },
                {
                    maxContentLength: Infinity,
                    maxBodyLength: Infinity,
                    auth: {
                        username: this.elasticsearchUser,
                        password: this.elasticsearchPassword
                    },
                    httpsAgent: this.agent
                });
            return {
                ...item,
                seqNo: resp.data._seq_no,
                primaryTerm: resp.data._primary_term
            };
        } catch (err: any) {
            if (err.response && (err.response.status === 409 || err.response.status === 404)) {
                return undefined;
            }
            throw err;
        }
    }

    async complete(item: ChunkQueueItem): Promise<void> {
        return Axios.delete(`${this.elasticsearchHost}/${this.queueIndex}/_doc/${encodeURIComponent(item.id)}?refresh=true`, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            auth: {
                username: this.elasticsearchUser,
                password: this.elasticsearchPassword
            },
            httpsAgent: this.agent
        }).then(() => {
            console.log(`completed chunk job for document ${item.documentId}`);
        }).catch(err => {
            if (err.response && err.response.data && err.response.data.error) {
                throw { document: item.documentId, response: err.response.data.error };
            } else {
                throw { document: item.documentId, response: err };
            }
        });
    }

    async fail(item: ChunkQueueItem, attempts: number, maxAttempts: number, error: any): Promise<void> {
        const exhausted = attempts >= maxAttempts;
        await Axios.post(`${this.elasticsearchHost}/${this.queueIndex}/_update/${encodeURIComponent(item.id)}`, {
            doc: {
                status: exhausted ? ChunkQueueStatus.FAILED : ChunkQueueStatus.PENDING,
                attempts: attempts,
                error: typeof error === 'string' ? error : JSON.stringify(error),
                updatedAt: new Date().toISOString()
            }
        }, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            auth: {
                username: this.elasticsearchUser,
                password: this.elasticsearchPassword
            },
            httpsAgent: this.agent
        }).then(() => {
            console.log(`chunk job for document ${item.documentId} ${exhausted ? 'failed permanently' : 'requeued'} after ${attempts} attempt(s)`);
        }).catch(err => {
            console.error(`failed to record failure for document ${item.documentId}:`, err.response?.data?.error ?? err);
        });
    }
}
