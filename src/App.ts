import express, { Request, Response } from "express";
import cors from "cors";
import { SpiderUpdate } from "./Model";
import { SpiderProcessor } from "./SpiderProcessor";

const app = express()
const processor = new SpiderProcessor();

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
    console.log(`${new Date().toISOString()} error in processing spider ${spiderUpdate.spider} with timestamp ${spiderUpdate.time}: ${JSON.stringify(err)}`);
    return res.status(500).json(err);
  }
});

const port = process.env.PORT || 8000;

app.listen(port,()=>{
  console.log('Server Started at Port, ' + port);
})
