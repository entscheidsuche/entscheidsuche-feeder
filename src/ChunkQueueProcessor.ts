import { ChunkProcessor } from "./ChunkProcessor";
import { ChunkQueue, ChunkQueueItem } from "./ChunkQueue";

export class ChunkQueueProcessor {

    private queue: ChunkQueue;
    private chunkProcessor: ChunkProcessor;
    private readonly pollIntervalMs: number;
    private readonly batchSize: number;
    private readonly maxAttempts: number;
    private running: boolean = false;

    constructor(queue: ChunkQueue, chunkProcessor: ChunkProcessor) {
        this.queue = queue;
        this.chunkProcessor = chunkProcessor;
        this.pollIntervalMs = parseInt(`${process.env.CHUNK_QUEUE_POLL_INTERVAL_MS || 5000}`);
        this.batchSize = parseInt(`${process.env.CHUNK_QUEUE_BATCH_SIZE || 1}`);
        this.maxAttempts = parseInt(`${process.env.CHUNK_QUEUE_MAX_ATTEMPTS || 3}`);
    }

    start(): void {
        if (this.running) {
            return;
        }
        this.running = true;
        console.log(`chunk queue processor started (interval ${this.pollIntervalMs} ms, batch ${this.batchSize})`);
        void this.loop();
    }

    stop(): void {
        this.running = false;
    }

    private async loop(): Promise<void> {
        while (this.running) {
            let processedAny = false;
            try {
                processedAny = await this.processBatch();
            } catch (err) {
                console.error(`error while processing chunk queue:`, err);
            }
            if (!processedAny) {
                await this.sleep(this.pollIntervalMs);
            }
        }
    }

    private async processBatch(): Promise<boolean> {
        const items = await this.queue.claimBatch(this.batchSize);
        if (items.length === 0) {
            return false;
        }
        for (const item of items) {
            await this.processItem(item);
        }
        return true;
    }

    private async processItem(item: ChunkQueueItem): Promise<void> {
        try {
            await this.chunkProcessor.process(item.documentId);
            await this.queue.complete(item);
        } catch (err) {
            console.error(`failed to process chunk job for document ${item.documentId}:`, err);
            await this.queue.fail(item, item.attempts + 1, this.maxAttempts, err);
        }
    }

    private sleep(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}
