import express, { Request, Response } from "express";
import cors from "cors";
import { SpiderUpdate } from "./Model";
import { SpiderProcessor } from "./SpiderProcessor";
import { serializeError } from "serialize-error";
import {ChunkProcessor} from "./ChunkProcessor";

const app = express()
const processor = new SpiderProcessor();
const chunkProcessor = new ChunkProcessor();

app.use(cors());
app.use(express.json({ limit: '100mb' }));


app.get("/", (req: Request, res: Response) => {
  res.status(200).send("use post method to upload a spider file");
})

app.post("/", async (req, res) => {
  const spiderUpdate:SpiderUpdate = req.body;
  console.log(`${new Date().toISOString()} processing spider ${spiderUpdate.spider} with timestamp ${spiderUpdate.time}`);
  try {
    await processor.process(spiderUpdate);
    console.log(`${new Date().toISOString()} finished processing spider ${spiderUpdate.spider} with timestamp ${spiderUpdate.time}`);

    return res.status(201).send();
  } catch (err) {
    console.log(`${new Date().toISOString()} error in processing spider ${spiderUpdate.spider} with timestamp ${spiderUpdate.time}: ${JSON.stringify(serializeError(err))}`);
    return res.status(500).json(serializeError(err));
  }
});

app.post("/chunk", async (req, res) => {
    const dokId = req.body;
    console.log(`${new Date().toISOString()} processing chunks for document ${dokId.id}`);
    try {
        await chunkProcessor.process(dokId.id);
        console.log(`${new Date().toISOString()} finished processing chunks for ${dokId.id}`);
        return res.status(200).send();
    }
    catch (err) {
        console.log(`${new Date().toISOString()} error in processing chunks for ${dokId.id}`);
        return res.status(500).json(serializeError(err));
    }

})


app.get("/import", async (req, res) => {
    try {
        await chunkProcessor.importAll()
        return res.status(200).send();
    }
    catch (err) {
        console.log(`${new Date().toISOString()} error in processing import`);
        return res.status(500).json(serializeError(err));
    }
})


app.get("/createChunkIndex", async (req, res) => {
    try {
        await chunkProcessor.createOrUpdateEmbeddingIndex("embeddings_qwen3-embedding")
        return res.status(200).send();
    }
    catch (err) {
        console.log(`${new Date().toISOString()} error in processing import`);
        return res.status(500).json(serializeError(err));
    }
})


app.get("/createMicroChunkIndex", async (req, res) => {
    try {
        await chunkProcessor.createOrUpdateMicroChunkIndex("embeddings_qwen3-embedding_micro")
        return res.status(200).send();
    }
    catch (err) {
        console.log(`${new Date().toISOString()} error in processing import`);
        return res.status(500).json(serializeError(err));
    }
})

const port = process.env.PORT || 8000;

app.listen(port,()=>{
  console.log('Server Started at Port, ' + port);
})
