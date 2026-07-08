import Axios from "axios";
import { ElasticUtil } from "./ElasticUtil";
import { ReportingUtil } from "./ReportingUtil";
import { SpiderUpdate } from "./Model";
import { serializeError } from "serialize-error";
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
    spider: string;
    job: string;
    attempts: number;
    seqNo: number;
    primaryTerm: number;
}

interface FailedDocument {
    documentId: string;
    error: any;
}

interface JobProgress {
    spiderUpdate: SpiderUpdate;
    enqueued: number;
    processed: number;
    failed: number;
    failedDocuments: Array<FailedDocument>;
    enqueueComplete: boolean;
    reported: boolean;
}

export interface JobQueueStatus {
    spider: string;
    job: string;
    total: number;
    remaining: number;
    processed: number;
    failed: number;
    enqueueComplete: boolean;
}

export interface QueueStatus {
    queues: Array<JobQueueStatus>;
    totals: {
        queues: number;
        total: number;
        remaining: number;
    };
}

export class ChunkQueue {

    private elasticsearchHost: string;
    private elasticsearchUser: string;
    private elasticsearchPassword: string;
    private readonly queueIndex: string;
    private agent: https.Agent;
    private elasticUtil: ElasticUtil;
    private reportingUtil: ReportingUtil;
    private indexReady: Promise<void> | undefined;
    private jobs: Map<string, JobProgress> = new Map();
    private abandonedJobs: Set<string> = new Set();
    private readonly maxJobFailures: number;
    private readonly processingTimeoutMs: number;

    constructor() {
        this.elasticsearchHost = `${process.env.ELASTICSEARCH_HOST}`;
        this.elasticsearchUser = `${process.env.ELASTICSEARCH_USER}`;
        this.elasticsearchPassword = `${process.env.ELASTICSEARCH_PASSWORD}`;
        this.queueIndex = `${process.env.CHUNK_QUEUE_INDEX || 'chunk_queue'}`;
        this.maxJobFailures = parseInt(`${process.env.CHUNK_QUEUE_MAX_JOB_FAILURES || 100}`);
        this.processingTimeoutMs = parseInt(`${process.env.CHUNK_QUEUE_PROCESSING_TIMEOUT_MS || 3600000}`);
        this.elasticUtil = new ElasticUtil();
        this.reportingUtil = new ReportingUtil();
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
                                spider: { type: "keyword" },
                                job: { type: "keyword" },
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

    async enqueue(documentId: string, spiderUpdate: SpiderUpdate): Promise<void> {
        await this.ensureIndex();
        const now = new Date().toISOString();
        const id = encodeURIComponent(documentId);
        return Axios.put(`${this.elasticsearchHost}/${this.queueIndex}/_doc/${id}`, {
            documentId: documentId,
            spider: spiderUpdate.spider,
            job: spiderUpdate.job,
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
            this.registerEnqueued(spiderUpdate);
            console.log(`enqueued chunk job for document ${documentId}`);
        }).catch(err => {
            if (err.response && err.response.data && err.response.data.error) {
                throw { document: documentId, response: err.response.data.error };
            } else {
                throw { document: documentId, response: err };
            }
        });
    }

    async finishEnqueue(spiderUpdate: SpiderUpdate): Promise<void> {
        const key = ChunkQueue.jobKey(spiderUpdate.spider, spiderUpdate.job);
        const progress = this.jobs.get(key);
        if (progress === undefined) {
            await this.reportingUtil.reportStatus(spiderUpdate, undefined, 'chunk');
            return;
        }
        progress.enqueueComplete = true;
        await this.evaluateJob(key);
    }


    getQueueStatus(): QueueStatus {
        const queues: Array<JobQueueStatus> = [];
        let totalDocuments = 0;
        let remainingDocuments = 0;
        this.jobs.forEach(progress => {
            const remaining = Math.max(progress.enqueued - progress.processed, 0);
            queues.push({
                spider: progress.spiderUpdate.spider,
                job: progress.spiderUpdate.job,
                total: progress.enqueued,
                remaining: remaining,
                processed: progress.processed,
                failed: progress.failed,
                enqueueComplete: progress.enqueueComplete
            });
            totalDocuments += progress.enqueued;
            remainingDocuments += remaining;
        });
        return {
            queues: queues,
            totals: {
                queues: queues.length,
                total: totalDocuments,
                remaining: remainingDocuments
            }
        };
    }

    private static jobKey(spider: string, job: string): string {
        return `${spider}::${job}`;
    }

    private registerEnqueued(spiderUpdate: SpiderUpdate): void {
        const key = ChunkQueue.jobKey(spiderUpdate.spider, spiderUpdate.job);
        if (this.abandonedJobs.has(key)) {
            return;
        }
        let progress = this.jobs.get(key);
        if (progress === undefined) {
            progress = {
                spiderUpdate: spiderUpdate,
                enqueued: 0,
                processed: 0,
                failed: 0,
                failedDocuments: [],
                enqueueComplete: false,
                reported: false
            };
            this.jobs.set(key, progress);
        }
        progress.enqueued++;
    }

    private async recordResult(item: ChunkQueueItem, failed: boolean, error?: any): Promise<void> {
        const key = ChunkQueue.jobKey(item.spider, item.job);
        const progress = this.jobs.get(key);
        if (progress === undefined) {
            return;
        }
        progress.processed++;
        if (failed) {
            progress.failed++;
            progress.failedDocuments.push({
                documentId: item.documentId,
                error: ChunkQueue.stripStack(serializeError(error))
            });
        }
        await this.evaluateJob(key);
    }

    private async evaluateJob(key: string): Promise<void> {
        const progress = this.jobs.get(key);
        if (progress === undefined || progress.reported) {
            return;
        }
        if (progress.failed > this.maxJobFailures) {
            progress.reported = true;
            this.jobs.delete(key);
            this.abandonedJobs.add(key);
            console.error(`more than ${this.maxJobFailures} documents failed chunk processing for spider ${progress.spiderUpdate.spider}, job ${progress.spiderUpdate.job}; abandoning this job`);
            await this.purgeJob(progress.spiderUpdate.spider, progress.spiderUpdate.job);
            const message = ChunkQueue.buildFailureMessage(progress,
                `chunk processing abandoned: ${progress.failed} of ${progress.processed} documents failed (threshold ${this.maxJobFailures})`);
            await this.reportingUtil.report(progress.spiderUpdate, 'error', message, 'chunk');
            return;
        }
        if (progress.enqueueComplete && progress.processed >= progress.enqueued) {
            progress.reported = true;
            this.jobs.delete(key);
            if (progress.failed > 0) {
                const message = ChunkQueue.buildFailureMessage(progress,
                    `${progress.failed} of ${progress.processed} documents failed chunk processing`);
                await this.reportingUtil.report(progress.spiderUpdate, 'warning', message, 'chunk');
            } else {
                await this.reportingUtil.reportStatus(progress.spiderUpdate, undefined, 'chunk');
            }
        }
    }

    private static stripStack(value: any): any {
        if (Array.isArray(value)) {
            return value.map(entry => ChunkQueue.stripStack(entry));
        }
        if (value !== null && typeof value === 'object') {
            const result: any = {};
            for (const key of Object.keys(value)) {
                if (key === 'stack') {
                    continue;
                }
                result[key] = ChunkQueue.stripStack(value[key]);
            }
            return result;
        }
        return value;
    }

    private static buildFailureMessage(progress: JobProgress, note: string): string {
        return JSON.stringify({
            message: note,
            spider: progress.spiderUpdate.spider,
            job: progress.spiderUpdate.job,
            failed: progress.failed,
            processed: progress.processed,
            failedDocuments: progress.failedDocuments
        });
    }

    private async purgeJob(spider: string, job: string): Promise<void> {
        await Axios.post(`${this.elasticsearchHost}/${this.queueIndex}/_delete_by_query?conflicts=proceed&refresh=true`, {
            query: {
                bool: {
                    filter: [
                        { term: { spider: spider } },
                        { term: { job: job } }
                    ]
                }
            }
        }, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            auth: {
                username: this.elasticsearchUser,
                password: this.elasticsearchPassword
            },
            httpsAgent: this.agent
        }).then(resp => {
            console.log(`purged ${resp.data?.deleted ?? 0} remaining queue items for abandoned spider ${spider}, job ${job}`);
        }).catch(err => {
            console.error(`failed to purge queue items for spider ${spider}, job ${job}:`, err.response?.data?.error ?? err);
        });
    }

    async claimBatch(batchSize: number, maxAttempts: number): Promise<Array<ChunkQueueItem>> {
        await this.ensureIndex();
        const staleBefore = new Date(Date.now() - this.processingTimeoutMs).toISOString();
        const query = {
            size: batchSize,
            query: {
                bool: {
                    should: [
                        { term: { status: ChunkQueueStatus.PENDING } },
                        {
                            bool: {
                                filter: [
                                    { term: { status: ChunkQueueStatus.PROCESSING } },
                                    { range: { updatedAt: { lte: staleBefore } } }
                                ]
                            }
                        }
                    ],
                    minimum_should_match: 1
                }
            },
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
                spider: hit._source.spider,
                job: hit._source.job,
                attempts: hit._source.attempts ?? 0,
                seqNo: hit._seq_no,
                primaryTerm: hit._primary_term
            };
            const isRetry = hit._source.status === ChunkQueueStatus.PROCESSING;
            if (isRetry) {
                const nextAttempts = item.attempts + 1;
                if (nextAttempts > maxAttempts) {
                    // Timed out again after exhausting all attempts -> mark it permanently failed.
                    await this.fail(item, nextAttempts, maxAttempts, 'processing timed out');
                    continue;
                }
                const result = await this.markProcessing(item, nextAttempts);
                if (result !== undefined) {
                    claimed.push(result);
                }
            } else {
                const result = await this.markProcessing(item);
                if (result !== undefined) {
                    claimed.push(result);
                }
            }
        }
        return claimed;
    }

    private async markProcessing(item: ChunkQueueItem, attempts?: number): Promise<ChunkQueueItem | undefined> {
        const doc: any = {
            status: ChunkQueueStatus.PROCESSING,
            updatedAt: new Date().toISOString()
        };
        if (attempts !== undefined) {
            doc.attempts = attempts;
        }
        try {
            const resp = await Axios.post(
                `${this.elasticsearchHost}/${this.queueIndex}/_update/${encodeURIComponent(item.id)}` +
                `?if_seq_no=${item.seqNo}&if_primary_term=${item.primaryTerm}`,
                { doc: doc },
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
                attempts: attempts ?? item.attempts,
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
        }).then(async () => {
            console.log(`completed chunk job for document ${item.documentId}`);
            await this.recordResult(item, false);
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
        if (exhausted) {
            await this.recordResult(item, true, error);
        }
    }
}
