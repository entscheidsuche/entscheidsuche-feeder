import Axios from "axios";
import {errorInfo} from "./ErrorUtil";

// Client for an OpenAI-compatible /v1/embeddings endpoint (vLLM). Sends texts in batches so the
// server can embed many inputs per forward pass instead of one request per chunk.
export class EmbeddingClient {

    private readonly apiUrl: string;
    private readonly model: string;
    private readonly batchSize: number;
    private readonly maxRetries: number;
    private readonly priority: number;

    constructor(apiUrl: string, model: string) {
        this.apiUrl = apiUrl;
        this.model = model;
        this.batchSize = parseInt(`${process.env.EMBED_BATCH_SIZE || 32}`);
        this.maxRetries = parseInt(`${process.env.EMBED_MAX_RETRIES || 3}`);
        // Higher value = handled later. Lets search queries (priority 0) skip ahead of indexing batches.
        // Only takes effect if vLLM runs with --scheduling-policy priority; otherwise it must stay 0.
        this.priority = parseInt(`${process.env.EMBED_PRIORITY || 0}`);
    }

    async embed(texts: Array<string>): Promise<Array<Array<number>>> {
        // vLLM rejects the whole batch if one input is not a string, with a confusing validation error.
        const invalid = texts.findIndex(text => typeof text !== 'string');
        if (invalid !== -1) {
            throw new Error(`embedding input ${invalid} is a ${typeof texts[invalid]}, not a string: ${JSON.stringify(texts[invalid])}`.slice(0, 300));
        }
        const embeddings: Array<Array<number>> = [];
        for (let start = 0; start < texts.length; start += this.batchSize) {
            const batch = texts.slice(start, start + this.batchSize);
            embeddings.push(...await this.embedBatch(batch));
        }
        return embeddings;
    }

    private async embedBatch(batch: Array<string>): Promise<Array<Array<number>>> {
        const body: any = {input: batch, model: this.model, encoding_format: "float"};
        if (this.priority !== 0) {
            body.priority = this.priority;
        }
        for (let attempt = 0; ; attempt++) {
            try {
                const resp = await Axios.post(this.apiUrl + '/v1/embeddings', body, {
                    timeout: 600000,
                    maxContentLength: Infinity,
                    maxBodyLength: Infinity
                });
                const data: Array<{index: number, embedding: Array<number>}> = resp.data.data;
                if (data.length !== batch.length) {
                    throw new Error(`expected ${batch.length} embeddings from ${this.model}, got ${data.length}`);
                }
                return data.sort((a, b) => a.index - b.index).map(item => item.embedding);
            } catch (err: any) {
                // Retry only on server errors and connection problems, not on bad requests (4xx).
                const status = err.response?.status;
                const retryable = status === undefined || status >= 500;
                if (!retryable || attempt >= this.maxRetries) {
                    const info = errorInfo(err);
                    console.log(`embedding request to ${this.model} failed: ${JSON.stringify(info)}`);
                    throw info;
                }
                const delayMs = 1000 * Math.pow(2, attempt);
                console.log(`embedding request to ${this.model} failed (${err.message}), retrying in ${delayMs} ms`);
                await new Promise(resolve => setTimeout(resolve, delayMs));
            }
        }
    }
}
