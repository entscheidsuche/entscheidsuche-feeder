import express, { Request, Response } from "express";
import cors from "cors";
import Axios from "axios";
import { SpiderUpdate } from "./Model";
import { SpiderProcessor } from "./SpiderProcessor";
import { serializeError } from "serialize-error";

const app = express()
const processor = new SpiderProcessor();

app.use(cors());
app.use(express.json({ limit: '100mb' }));

// Status-Vermittler feeder_status.php: der Feeder meldet das Ergebnis eines Requests.
// index ist eine Eigenschaft DIESES Feeders ("2" live / "3" kuenftiger KI-Index) und
// wird NICHT im Request uebergeben; die Jobs-Datei ist fuer beide Indexer identisch.
const FEEDER_STATUS_URL = process.env.FEEDER_STATUS_URL || "https://entscheidsuche.ch/feeder_status.php";
const FEEDER_INDEX = process.env.FEEDER_INDEX || "2";

// Ergebnis eines Requests an feeder_status.php melden. Nur wenn der Konsolidierer einen
// token (= rtoken) als Query-Parameter mitgeschickt hat; ohne token wird KEIN Status
// geschrieben (erlaubt manuelle Aufrufe des Feeders). Ein Fehler beim Melden darf den
// Request NICHT kippen -> gefangen und nur geloggt. axios url-encodet die Parameter.
async function reportStatus(token: string | undefined, spiderUpdate: SpiderUpdate, status: "ok" | "error", message?: string) {
  if (!token) { return; }
  try {
    await Axios.get(FEEDER_STATUS_URL, {
      params: {
        index: FEEDER_INDEX,
        spider: spiderUpdate.spider,
        job: spiderUpdate.job,
        status,
        rtoken: token,
        ...(message ? { message: message.substring(0, 500) } : {}),
      },
      timeout: 30000,
    });
  } catch (e) {
    console.log(`${new Date().toISOString()} feeder_status report (${status}) failed for spider ${spiderUpdate.spider}: ${e}`);
  }
}

app.get("/", (req: Request, res: Response) => {
  res.status(200).send("use post method to upload a spider file");
})

app.post("/", async (req, res) => {
  const spiderUpdate:SpiderUpdate = req.body;
  // rtoken kommt als Query-Parameter 'token' (nicht im Body -> Body bleibt die Jobs-Datei).
  // Fehlt er, indexiert der Feeder normal, meldet aber keinen Status.
  const token = typeof req.query.token === "string" ? req.query.token : undefined;
  console.log(`${new Date().toISOString()} processing spider ${spiderUpdate.spider} with timestamp ${spiderUpdate.time}`);
  try {
    await processor.process(spiderUpdate);
    console.log(`${new Date().toISOString()} finished processing spider ${spiderUpdate.spider} with timestamp ${spiderUpdate.time}`);
    // WICHTIG: Status VOR der Antwort melden. Die Antwort kann bei einem Client-/Gateway-
    // Timeout abgebrochen werden; der Status muss dann trotzdem gesetzt sein.
    await reportStatus(token, spiderUpdate, "ok");
    return res.status(201).send();
  } catch (err) {
    console.log(`${new Date().toISOString()} error in processing spider ${spiderUpdate.spider} with timestamp ${spiderUpdate.time}: ${JSON.stringify(serializeError(err))}`);
    // Bei Dateifehlern enthaelt err.message "error building file <name>: ..." -> der
    // Konsolidierer parst daraus die schuldige Datei und markiert sie kaputt.
    const message = (err instanceof Error && err.message) ? err.message : JSON.stringify(serializeError(err));
    await reportStatus(token, spiderUpdate, "error", message);
    return res.status(500).json(serializeError(err));
  }
});

const port = process.env.PORT || 8000;

app.listen(port,()=>{
  console.log('Server Started at Port, ' + port);
})
