import {SpiderFiles, SpiderUpdate} from "./Model";
import Axios from "axios";
import {serializeError} from "serialize-error";

export type ReportStatus = 'ok' | 'warning' | 'error';

export type ReportPhase = 'insert' | 'chunk';

export class ReportingUtil {

    async reportStatus(spiderUpdate: SpiderUpdate, err?: any, phase: ReportPhase = 'insert'): Promise<void> {
        return this.report(
            spiderUpdate,
            err === undefined ? 'ok' : 'error',
            err === undefined ? 'ok' : JSON.stringify(serializeError(err)),
            phase
        );
    }

    async report(spiderUpdate: SpiderUpdate, status: ReportStatus, message: string, phase: ReportPhase = 'insert'): Promise<void> {
        return Axios.get(`${process.env.STATUS_REPORT_URL}`, {
            params: {
                index: `${process.env.FEEDER_ID}`,
                spider: spiderUpdate.spider,
                job: spiderUpdate.job,
                phase: phase,
                status: status,
                message: message
            }
        }).then(_ => {
            console.log(`reported ${phase} status ${status} for spider ${spiderUpdate.spider}, job ${spiderUpdate.job}`);
        }).catch(reportErr => {
            console.log(`error reporting status for spider ${spiderUpdate.spider}, job ${spiderUpdate.job}: ${JSON.stringify(serializeError(reportErr))}`);
        });
    }
}