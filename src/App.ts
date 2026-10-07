import express, { Request, Response } from "express";
import cors from "cors";
import { SpiderUpdate } from "./Model";
import { SpiderProcessor } from "./SpiderProcessor";
import { errorInfo } from "./ErrorUtil";
import {ChunkProcessor} from "./ChunkProcessor";
import {ChunkQueue} from "./ChunkQueue";
import {ChunkQueueProcessor} from "./ChunkQueueProcessor";
import {ReportingUtil} from "./ReportingUtil";

const app = express()
const chunkQueue = new ChunkQueue();
const processor = new SpiderProcessor(chunkQueue);
const chunkProcessor = new ChunkProcessor();
const chunkQueueProcessor = new ChunkQueueProcessor(chunkQueue, chunkProcessor);
const reportingUtil = new ReportingUtil();
chunkProcessor.ensureIndices()
    .catch(err => console.error(`failed to create embedding indices:`, err))
    .then(() => chunkQueueProcessor.start());

app.use(cors());
app.use(express.json({ limit: '100mb' }));


app.get("/", (req: Request, res: Response) => {
  res.status(200).send("use post method to upload a spider file");
})

app.get("/status", (req: Request, res: Response) => {
  res.status(200).json(chunkQueue.getQueueStatus());
})

app.post("/", async (req, res) => {
  const spiderUpdate:SpiderUpdate = req.body;
  console.log(`${new Date().toISOString()} processing spider ${spiderUpdate.spider} with timestamp ${spiderUpdate.time}`);
  try {
    await processor.process(spiderUpdate);
    console.log(`${new Date().toISOString()} finished processing spider ${spiderUpdate.spider} with timestamp ${spiderUpdate.time}`);
    await reportingUtil.reportStatus(spiderUpdate);
    return res.status(201).send();
  } catch (err) {
    console.log(`${new Date().toISOString()} error in processing spider ${spiderUpdate.spider} with timestamp ${spiderUpdate.time}: ${JSON.stringify(errorInfo(err))}`);
    await reportingUtil.reportStatus(spiderUpdate, err);
    return res.status(500).json(errorInfo(err));
  }
});

app.post("/chunk", async (req, res) => {
    const dokId = req.body;
    if (!dokId || typeof dokId.id !== 'string' || dokId.id === '') {
        return res.status(400).json({message: 'expected a JSON body {"id": "<document id>"} with Content-Type: application/json'});
    }
    console.log(`${new Date().toISOString()} processing chunks for document ${dokId.id}`);
    try {
        await chunkProcessor.process(dokId.id);
        console.log(`${new Date().toISOString()} finished processing chunks for ${dokId.id}`);
        return res.status(200).send();
    }
    catch (err) {
        console.log(`${new Date().toISOString()} error in processing chunks for ${dokId.id}: ${JSON.stringify(errorInfo(err))}`);
        return res.status(500).json(errorInfo(err));
    }

})


app.post("/indexMicroChunk", async (req, res) => {
    const reqBody = req.body;
    if (!reqBody || typeof reqBody.id !== 'string' || reqBody.id === '') {
        return res.status(400).json({message: 'expected a JSON body {"id": "<document id>", "chunkId"?: "<chunk id>"} with Content-Type: application/json'});
    }
    console.log(`${new Date().toISOString()} processing microchunks for document ${reqBody.id}`);
    try {
        if(reqBody.chunkId) {
            await chunkProcessor.indexMicroChunks(reqBody.id, reqBody.chunkId)
        }
        else {
            await chunkProcessor.indexMicroChunks(reqBody.id);
        }
        console.log(`${new Date().toISOString()} finished processing microchunks for ${reqBody.id}`);
        return res.status(200).send();
    }
    catch (err) {
        console.log(`${new Date().toISOString()} error in processing microchunks for ${reqBody.id}: ${JSON.stringify(errorInfo(err))}`);
        return res.status(500).json(errorInfo(err));
    }
})


app.post("/import", (req, res) => {
    if (chunkProcessor.isImportRunning()) {
        return res.status(409).json({message: 'an import is already running', status: chunkProcessor.getImportStatus()});
    }
    const params = req.body ?? {};
    // Runs for hours, so answer right away; progress is in the log and at GET /import/status.
    chunkProcessor.importAll(!!params.copyDocument, !!params.indexMicroChunks)
        .catch(err => console.log(`${new Date().toISOString()} error in processing import: ${JSON.stringify(errorInfo(err))}`));
    return res.status(202).json({message: 'import started'});
})

// All failed documents of the current/last import with their reasons, or only the ids (?format=ids).
app.get("/import/failed", (req, res) => {
    const failures = chunkProcessor.getImportFailures();
    if (failures === undefined) {
        return res.status(404).json({message: 'no import has run since the feeder started'});
    }
    if (req.query.format === 'ids') {
        return res.status(200).type('text/plain').send(failures.map(failure => failure.id).join('\n') + '\n');
    }
    return res.status(200).json(failures);
})

app.get("/import/status", (req, res) => {
    const status = chunkProcessor.getImportStatus();
    if (status === undefined) {
        return res.status(404).json({message: 'no import has run since the feeder started'});
    }
    return res.status(200).json(status);
})


app.get("/createChunkIndex", async (req, res) => {
    try {
        await chunkProcessor.createOrUpdateEmbeddingIndex(chunkProcessor.bigIndex)
        return res.status(200).send();
    }
    catch (err) {
        console.log(`${new Date().toISOString()} error in processing import`);
        return res.status(500).json(errorInfo(err));
    }
})


app.get("/createMicroChunkIndex", async (req, res) => {
    try {
        await chunkProcessor.createOrUpdateMicroChunkIndex(chunkProcessor.microIndex)
        return res.status(200).send();
    }
    catch (err) {
        console.log(`${new Date().toISOString()} error in processing import`);
        return res.status(500).json(errorInfo(err));
    }
})

const port = process.env.PORT || 8000;

app.listen(port,()=>{
  console.log('Server Started at Port, ' + port);
})
