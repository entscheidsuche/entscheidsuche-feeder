import {ELDocument, SpiderFiles} from "./Model";
import {fileLoader, FileLoader} from "./FileLoader";
import * as stream from "stream";
import {parseStringPromise} from "xml2js";
// @ts-ignore
import {get} from "lodash";


export class DocumentBuilder {

    private fileLoader:FileLoader;

    constructor() {
        this.fileLoader = fileLoader();
    }

    async build(spiderFiles:SpiderFiles): Promise<ELDocument> {
        let metaFileName = DocumentBuilder.getMetaFileName(spiderFiles);
        let attachmentFileName = DocumentBuilder.getPreferredAttachmentFileName(spiderFiles);
        if (attachmentFileName !== undefined) {
            return Promise.all([this.buildBaseDocument(metaFileName), this.getAttachment(attachmentFileName)])
                .then(zip => {
                    let [document, attachment] = zip;
                    document.data = attachment;
                    return document;
                });
        }
        return this.buildBaseDocument(metaFileName);
    }

    private async buildBaseDocument(metaFileName:string): Promise<ELDocument> {
        return this.streamToString(this.fileLoader.getStream(metaFileName), "utf-8")
            .then(xml => parseStringPromise(xml, {explicitArray: false})
                .then(metaData => DocumentBuilder.createBaseDocument(metaFileName, metaData)));
    }

    private async getAttachment(attachmentFileName:string): Promise<string> {
        return this.streamToString(this.fileLoader.getStream(attachmentFileName), "base64");
    }

    private static createBaseDocument(metaFileName: string, metaData:any): ELDocument {
        let levels = this.extractProperty(metaData, "Entscheid.Metainfos.Signatur")!.split("_");
        const hierarchie:Array<string> = [];
        let level_path:string = "";
        levels.forEach(level => {
            level_path = (level_path === "") ? level : `${level_path}_${level}`;
            hierarchie.push(level_path);
        });
        return {
            id: this.getDocumentId(metaFileName),
            titel: this.extractProperty(metaData, "Entscheid.Treffer.Kurz.Titel")!,
            leitsatz: this.extractProperty(metaData, "Entscheid.Treffer.Kurz.Leitsatz", value => value !== ""),
            rechtsgebiet: this.extractProperty(metaData, "Entscheid.Treffer.Kurz.Rechtsgebiet")!,
            hierarchie: hierarchie,
            signatur: this.extractProperty(metaData, "Entscheid.Metainfos.Signatur")!,
            kanton: this.extractProperty(metaData, "Entscheid.Metainfos.Kanton")!,
            gericht: this.extractProperty(metaData, "Entscheid.Metainfos.Gericht")!,
            geschaeftsnummer: this.extractProperty(metaData, "Entscheid.Metainfos.Geschaeftsnummer")!,
            edatum: this.extractProperty(metaData, "Entscheid.Metainfos.EDatum", value => value !== "0000-00-00")!
        };
    }

    private static extractProperty(metaData:any, path:string, filter?:(value:string) => boolean): string | undefined {
        let property = get(metaData, path);
        if (filter !== undefined && !filter(property)) {
            return undefined;
        }
        return property;
    }

    private static getMetaFileName(spiderFiles:SpiderFiles): string {
        for (let fileName in spiderFiles) {
            if (spiderFiles.hasOwnProperty(fileName) && fileName.endsWith(".xml")) {
                return fileName;
            }
        }
        throw new Error("missing xml in " + spiderFiles);
    }

    private static getPreferredAttachmentFileName(spiderFiles: SpiderFiles): string | undefined {
        for (let fileName in spiderFiles) {
            if (spiderFiles.hasOwnProperty(fileName) && fileName.endsWith(".pdf")) {
                return fileName;
            }
        }
        for (let fileName in spiderFiles) {
            if (spiderFiles.hasOwnProperty(fileName) && fileName.endsWith(".html")) {
                return fileName;
            }
        }
    }

    private async streamToString(stream:stream.Readable, encoding:BufferEncoding): Promise<string> {
        const chunks:Array<Uint8Array> = [];
        return new Promise((resolve, reject) => {
            stream.on('data', chunk => chunks.push(chunk));
            stream.on('error', reject);
            stream.on('end', () => resolve(Buffer.concat(chunks).toString(encoding)));
        });
    }

    private static getDocumentId(metaFileName: string): string {
        return metaFileName.substring(metaFileName.lastIndexOf("/") + 1, metaFileName.lastIndexOf("."));
    }
}
