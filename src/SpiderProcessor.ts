import {ELDocument, SpiderFiles, SpiderFileStatus, SpiderUpdate} from "./Model";
import {DocumentBuilder} from "./DocumentBuilder";
import Axios from "axios"

export class SpiderProcessor {

    private parallel:number = 16;
    private documentBuilder:DocumentBuilder;
    private elasticsearchBase:string;

    constructor() {
        this.documentBuilder = new DocumentBuilder();
        this.elasticsearchBase = `${process.env.ELASTICSEARCH_HOST}/${process.env.ELASTICSEARCH_INDEX}`;
    }

    async process(spiderUpdate:SpiderUpdate): Promise<void> {
        const spiderFilesList = SpiderProcessor.filterSpiderFiles(spiderUpdate).reverse();
        return this.processFiles(spiderFilesList);
    }

    async processFiles(spiderFilesList:Array<SpiderFiles>): Promise<void> {
        if (spiderFilesList.length === 0) {
            return Promise.resolve();
        }
        const processingSpiderFiles:Array<Promise<void>> = [];
        for (let idx = 0; idx < this.parallel; idx++) {
            let spiderFiles = spiderFilesList.pop();
            if (spiderFiles === undefined) {
                break;
            }
            processingSpiderFiles.push(
                this.documentBuilder.build(spiderFiles)
                    .then(doc => this.upsert(doc)));
        }
        return Promise.all(processingSpiderFiles).then(_ => this.processFiles(spiderFilesList));
    }

    async upsert(document:ELDocument): Promise<void> {
        if (!document.hasOwnProperty('data')) {
            const { id, ...data } = document;
            return Axios.post(`${this.elasticsearchBase}/_update/${id}`, {doc: data})
                .then(resp => {})
                .catch(err => console.log(err));
        } else {
            const { id, ...data } = document;
            return Axios.put(`${this.elasticsearchBase}/_doc/${id}/?pipeline=attachment`, data)
                .then(resp => {})
                .catch(err => console.log(err));
        }
    }

    private static filterSpiderFiles(spiderUpdate:SpiderUpdate): Array<SpiderFiles> {
        const spiderFilesList: Array<SpiderFiles> = [];

        let filePath:string = '';
        let spiderFiles: SpiderFiles = {};

        for (let fileName in spiderUpdate.dateien) {
            if (spiderUpdate.dateien.hasOwnProperty(fileName)) {
                let spiderFile = spiderUpdate.dateien[fileName];

                if (spiderFile.status != SpiderFileStatus.EQUAL) {
                    if (Object.keys(spiderFiles).length == 0) {
                        spiderFiles[fileName] = spiderFile;
                        filePath = fileName.substring(0, fileName.lastIndexOf('.'));
                    } else {
                        if (filePath === fileName.substring(0, fileName.lastIndexOf('.'))) {
                            spiderFiles[fileName] = spiderFile;
                        } else {
                            spiderFilesList.push(spiderFiles);
                            spiderFiles = {[fileName]: spiderFile};
                            filePath = fileName.substring(0, fileName.lastIndexOf('.'));
                        }
                    }
                }
            }
        }
        if (Object.keys(spiderFiles).length > 0) {
            spiderFilesList.push(spiderFiles);
        }
        return spiderFilesList;
    }
}
