import { ELDocument, IntText, SpiderFiles, SpiderFileStatus, SpiderUpdate } from "./Model";
import { fileLoader, FileLoader } from "./FileLoader";
import * as stream from "stream";
// @ts-ignore
import { get } from "lodash";


export class DocumentBuilder {

    private fileLoader:FileLoader;
    private documentBaseURL:string;

    constructor() {
        this.fileLoader = fileLoader();
        this.documentBaseURL = `${process.env.DOCUMENT_BASE_URL}`;
    }

    async build(spiderUpdate: SpiderUpdate, spiderFiles: SpiderFiles): Promise<ELDocument> {
        const [metaFileName, status] = DocumentBuilder.getMetaFileName(spiderFiles);
        const attachmentFileName = DocumentBuilder.getPreferredAttachmentFileName(spiderFiles);
        if (status !== SpiderFileStatus.DELETED) {
            if (attachmentFileName !== undefined) {
                return Promise.all([this.buildBaseDocument(metaFileName), this.getAttachment(attachmentFileName)])
                    .then(zip => {
                        let [document, attachment] = zip;
                        document.data = attachment;
                        if (document.url === undefined) {
                            document.url = `${this.documentBaseURL}/${attachmentFileName}`
                        }
                        return document;
                    });
            }
            return this.buildBaseDocument(metaFileName);
        }
        return this.buildDeletedDocument(metaFileName);
    }

    private async buildBaseDocument(metaFileName:string): Promise<ELDocument> {
        return this.streamToString(this.fileLoader.getStream(metaFileName), "utf-8")
            .then(json => JSON.parse(json))
                .then(metaData => DocumentBuilder.createBaseDocument(metaFileName, metaData))
            .catch(err => { throw new Error(`error building file ${metaFileName}: ${err}`) });
    }

    private async buildDeletedDocument(metaFileName:string): Promise<ELDocument> {
        return new Promise<ELDocument>((res, rej) => {
            const doc: ELDocument = {
                id: DocumentBuilder.getDocumentId(metaFileName),
                deleted: true
            };
            res(doc)
        });
    }

    private async getAttachment(attachmentFileName:string): Promise<string> {
        return this.streamToString(this.fileLoader.getStream(attachmentFileName), "base64")
            .catch(err => { throw new Error(`error building file ${attachmentFileName}: ${err}`) });
    }

    private static createBaseDocument(metaFileName: string, metaData:any): ELDocument {
        let levels = (metaData.Signatur as string).split("_");
        const hierarchy:Array<string> = [];
        let level_path:string = "";
        levels.forEach(level => {
            level_path = (level_path === "") ? level : `${level_path}_${level}`;
            hierarchy.push(level_path);
        });
        const canton = levels[0]
        const title = metaData.Kopfzeile !== undefined ? this.intText(metaData.Kopfzeile) : undefined
        const abstract = metaData.Abstract !== undefined ? this.intText(metaData.Abstract) : undefined
        const meta = metaData.Meta !== undefined ? this.intText(metaData.Meta) : undefined
        const reference = metaData.Num !== undefined ? Array.isArray(metaData.Num) ? metaData.Num : [metaData.Num] : undefined
        const date = metaData.Datum !== undefined && metaData.Datum !== "0000-00-00" ? metaData.Datum : undefined

        const doc: ELDocument = {
            id: this.getDocumentId(metaFileName),
            deleted: false,
            canton,
            hierarchy
        };

        if (title !== undefined) {
            doc.title = title;
        }

        if (abstract !== undefined) {
            doc.abstract = abstract;
        }

        if (meta !== undefined) {
            doc.meta = meta;
        }

        if (reference !== undefined && reference.length > 0) {
            doc.reference = reference;
        }

        if (date !== undefined) {
            doc.date = date;
        }

        return doc;
    }

    private static intText(raw: Array<{ Sprachen: Array<string>; Text: string }>): IntText {
        const text: IntText = { de: '', fr: '', it: '' }
        for (const entry of raw) {
            for (const sprache of entry.Sprachen) {
                (text as any)[sprache] = entry.Text
            }
        }
        return text
    }

    private static getMetaFileName(spiderFiles:SpiderFiles): [string, SpiderFileStatus] {
        for (let fileName in spiderFiles) {
            if (spiderFiles.hasOwnProperty(fileName) && fileName.endsWith(".json")) {
                return [fileName, spiderFiles[fileName].status];
            }
        }
        throw new Error("missing json in " + spiderFiles);
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
