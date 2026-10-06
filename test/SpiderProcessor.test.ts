import assert from "assert";
process.env.LOADER_TYPE = "FILE";
process.env.FILE_BASE_PATH = ".";
process.env.ELASTICSEARCH_HOST = "http://localhost:9200";
process.env.ELASTICSEARCH_INDEX = "test";

import { SpiderProcessor } from "../src/SpiderProcessor";
import { DocumentBuilder } from "../src/DocumentBuilder";
import { HTTPSFileLoader } from "../src/FileLoader";
import { SpiderFileStatus, SpiderUpdate, SpiderFiles, ELDocument } from "../src/Model";

async function testDocumentIdFromFiles() {
    console.log("Running testDocumentIdFromFiles...");
    const files: SpiderFiles = {
        "CH_BGer/CH_BGer_007_7B-68-2026_2026-09-02.html": {
            checksum: "abc",
            status: SpiderFileStatus.NEW
        },
        "CH_BGer/CH_BGer_007_7B-68-2026_2026-09-02.json": {
            checksum: "def",
            status: SpiderFileStatus.NEW
        }
    };
    const docId = DocumentBuilder.getDocumentId(files);
    assert.strictEqual(docId, "CH_BGer_007_7B-68-2026_2026-09-02");

    const fallbackFiles: SpiderFiles = {
        "CH_BGer/CH_BGer_009_9C-643-2025_2026-08-31.html": {
            checksum: "xyz",
            status: SpiderFileStatus.NEW
        }
    };
    const fallbackId = DocumentBuilder.getDocumentId(fallbackFiles);
    assert.strictEqual(fallbackId, "CH_BGer_009_9C-643-2025_2026-08-31");

    const singleFileNameId = DocumentBuilder.getDocumentId("CH_BGer/CH_BGer_009_9C-643-2025_2026-08-31.json");
    assert.strictEqual(singleFileNameId, "CH_BGer_009_9C-643-2025_2026-08-31");
    console.log("✓ testDocumentIdFromFiles passed");
}

async function testProcessFilesBatchResilience() {
    console.log("Running testProcessFilesBatchResilience...");
    const processor = new SpiderProcessor();

    // Override parallel to 2 for easier batch testing
    (processor as any).parallel = 2;

    const upsertedDocs: string[] = [];
    (processor as any).upsert = async (index: string, spiderUpdate: SpiderUpdate, doc: ELDocument) => {
        upsertedDocs.push(doc.id);
    };

    // Mock document builder to fail on doc-2, succeed on doc-1, doc-3, doc-4
    (processor as any).documentBuilder = {
        build: async (spiderUpdate: SpiderUpdate, spiderFiles: SpiderFiles) => {
            const docId = DocumentBuilder.getDocumentId(spiderFiles);
            if (docId === "doc-2") {
                throw new Error("Simulated failure on doc-2 (e.g. 504 Gateway Timeout or corrupt PDF)");
            }
            return {
                id: docId,
                deleted: false
            } as ELDocument;
        }
    };

    const spiderUpdate: SpiderUpdate = {
        spider: "CH_BGer",
        job: "2815",
        jobtyp: "update",
        time: "2026-10-05T19:52:00Z",
        dateien: {} as any
    };

    // 4 documents across 2 batches (batch 1: doc-1, doc-2; batch 2: doc-3, doc-4)
    // Note: processFiles pops from the end of the array
    const spiderFilesList: Array<SpiderFiles> = [
        { "CH_BGer/doc-4.json": { checksum: "4", status: SpiderFileStatus.NEW } },
        { "CH_BGer/doc-3.json": { checksum: "3", status: SpiderFileStatus.NEW } },
        { "CH_BGer/doc-2.json": { checksum: "2", status: SpiderFileStatus.NEW } },
        { "CH_BGer/doc-1.json": { checksum: "1", status: SpiderFileStatus.NEW } }
    ];

    const errors = await processor.processFiles("test-index", spiderUpdate, spiderFilesList);

    // Verify doc-1 was upserted (batch 1)
    assert(upsertedDocs.includes("doc-1"), "doc-1 should have been upserted");

    // Verify doc-2 failed and recorded in errors
    assert.strictEqual(errors.length, 1, "There should be exactly 1 error recorded");
    assert.strictEqual(errors[0].document, "doc-2");
    assert(errors[0].error.message.includes("Simulated failure on doc-2"));

    // Crucially: verify batch 2 (doc-3 and doc-4) were NOT aborted and were successfully upserted!
    assert(upsertedDocs.includes("doc-3"), "doc-3 in next batch must NOT be skipped");
    assert(upsertedDocs.includes("doc-4"), "doc-4 in next batch must NOT be skipped");
    assert.strictEqual(upsertedDocs.length, 3, "Exactly 3 out of 4 documents must be upserted");

    console.log("✓ testProcessFilesBatchResilience passed");
}

async function testProcessThrowsWhenErrorsOccurred() {
    console.log("Running testProcessThrowsWhenErrorsOccurred...");
    const processor = new SpiderProcessor();

    (processor as any).getIndex = () => "test-index";
    (processor as any).fetchExistingSpider = async () => ({});
    (processor as any).processFiles = async () => [
        { document: "doc-failed", error: { message: "ES attachment rejected" } }
    ];

    const spiderUpdate: SpiderUpdate = {
        spider: "CH_BGer",
        job: "2815",
        jobtyp: "update",
        time: "2026-10-05T19:52:00Z",
        dateien: {
            "CH_BGer/doc-failed.json": { checksum: "1", status: SpiderFileStatus.NEW }
        }
    };

    let caughtError: any = null;
    try {
        await processor.process(spiderUpdate);
    } catch (err) {
        caughtError = err;
    }

    assert(caughtError !== null, "process() must throw when errors occurred");
    assert.strictEqual(caughtError.errors.length, 1);
    assert.strictEqual(caughtError.errors[0].document, "doc-failed");
    console.log("✓ testProcessThrowsWhenErrorsOccurred passed");
}

async function testHttpsFileLoaderErrorHandling() {
    console.log("Running testHttpsFileLoaderErrorHandling...");
    const loader = new HTTPSFileLoader("https://127.0.0.1:1");
    const stream = loader.getStream("test.json");
    let errorEmitted = false;
    await new Promise<void>((resolve) => {
        stream.on("error", () => {
            errorEmitted = true;
            resolve();
        });
        stream.resume();
    });
    assert(errorEmitted, "HTTPSFileLoader must emit error on connection failure");
    console.log("✓ testHttpsFileLoaderErrorHandling passed");
}

async function run() {
    try {
        await testDocumentIdFromFiles();
        await testHttpsFileLoaderErrorHandling();
        await testProcessFilesBatchResilience();
        await testProcessThrowsWhenErrorsOccurred();
        console.log("\nAll unit tests passed successfully!");
    } catch (err) {
        console.error("Test failure:", err);
        process.exit(1);
    }
}

run();
