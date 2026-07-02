import express, { Request, Response } from "express";
import cors from "cors";
import { SpiderUpdate } from "./Model";
import { SpiderProcessor } from "./SpiderProcessor";
import { serializeError } from "serialize-error";

const app = express()
const processor = new SpiderProcessor();

app.use(cors());
app.use(express.json({ limit: '100mb' }));


app.get("/", (req: Request, res: Response) => {
  res.status(200).send("use post method to upload a spider file");
})

app.post("/", async (req, res) => {
  const spiderUpdate:SpiderUpdate = req.body;
  // rtoken kommt als Query-Parameter 'token' (nicht im Body -> Body bleibt die Jobs-Datei).
  // Fehlt er, indexiert der Feeder normal, meldet aber keinen Status (erlaubt manuelle Aufrufe).
  const token = typeof req.query.token === "string" ? req.query.token : undefined;
  console.log(`${new Date().toISOString()} processing spider ${spiderUpdate.spider} with timestamp ${spiderUpdate.time}`);
  try {
    await processor.process(spiderUpdate);
    console.log(`${new Date().toISOString()} finished processing spider ${spiderUpdate.spider} with timestamp ${spiderUpdate.time}`);
    // Status VOR der Antwort melden: die Antwort kann bei einem Client-/Gateway-Timeout
    // abgebrochen werden, der Status muss dann trotzdem gesetzt sein.
    await processor.reportStatus(spiderUpdate, token);
    return res.status(201).send();
  } catch (err) {
    console.log(`${new Date().toISOString()} error in processing spider ${spiderUpdate.spider} with timestamp ${spiderUpdate.time}: ${JSON.stringify(serializeError(err))}`);
    await processor.reportStatus(spiderUpdate, token, err);
    return res.status(500).json(serializeError(err));
  }
});

const port = process.env.PORT || 8000;

app.listen(port,()=>{
  console.log('Server Started at Port, ' + port);
})
