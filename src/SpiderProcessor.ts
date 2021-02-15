import { ELDocument, SpiderDictionary, SpiderFiles, SpiderFileStatus, SpiderUpdate } from "./Model";
import { DocumentBuilder } from "./DocumentBuilder";
import Axios from "axios"

export class SpiderProcessor {

    private parallel: number = 16;
    private documentBuilder: DocumentBuilder;
    private elasticsearchHost: string;
    private readonly elasticsearchIndex: string;
    private elasticsearchUser: string;
    private elasticsearchPassword: string;

    constructor() {
        this.documentBuilder = new DocumentBuilder();
        this.elasticsearchHost = `${process.env.ELASTICSEARCH_HOST}`;
        this.elasticsearchIndex = `${process.env.ELASTICSEARCH_INDEX}`;
        this.elasticsearchUser = `${process.env.ELASTICSEARCH_USER}`;
        this.elasticsearchPassword = `${process.env.ELASTICSEARCH_PASSWORD}`;
    }

    async process(spiderUpdate: SpiderUpdate): Promise<void> {
        const index = this.getIndex(spiderUpdate);
        const xcurrentSequence = SpiderProcessor.getSequence(spiderUpdate.job);
        return this.fetchExistingSpider(spiderUpdate.spider, index)
            .then(spiderDictionary => SpiderProcessor.filterSpiderFiles(spiderDictionary, spiderUpdate).reverse())
            .then(spiderFilesList => this.processFiles(index, spiderUpdate, spiderFilesList));
    }

    async fetchExistingSpider(spider: string, index: string): Promise<SpiderDictionary> {
        const spiderDictionary: SpiderDictionary = {};
        return this.fetchExistingSpiderFrame(spider, index, spiderDictionary).then(_ => spiderDictionary);
    }

    async fetchExistingSpiderFrame(spider: string, index: string, spiderDictionary: SpiderDictionary, from?: Array<string>): Promise<void> {
        const query: any = {
            size: 1000,
            query: {
                match_all: {}
            },
            fields: ["source", "attachment.source", "attachment.content_type"],
            _source: false,
            sort: [ { id: "desc"} ],
        };
        if (from !== undefined) {
            query.search_after = from;
        }
        return Axios.post(`${this.elasticsearchHost}/${index}/_search`, query, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity
        })
            .then(resp => {
                if (resp.data.hits !== undefined && resp.data.hits.hits !== undefined) {
                    let lastSort: Array<string> | undefined;
                    const hits: Array<any> = resp.data.hits.hits;
                    for (let hit of hits) {
                        const id: string = hit._id;
                        if (hit.fields !== undefined) {
                            const fields = hit.fields;
                            if (fields.hasOwnProperty("source") && fields["source"].length > 0) {
                                spiderDictionary[`${spider}/${id}.json`] = SpiderProcessor.getSequence(fields["source"][0]);
                            }
                            if (fields.hasOwnProperty("attachment.source") && fields["attachment.source"].length > 0) {
                                const extension = fields.hasOwnProperty("attachment.content_type")
                                        && fields["attachment.content_type"].length > 0
                                        && fields["attachment.content_type"][0] === "application/pdf" ? "pdf" : "html";
                                spiderDictionary[`${spider}/${id}.${extension}`] = SpiderProcessor.getSequence(fields["attachment.source"][0]);
                            }
                        }
                        lastSort = hit.sort;
                    }
                    if (lastSort !== undefined) {
                        return this.fetchExistingSpiderFrame(spider, index, spiderDictionary, lastSort);
                    }
                }
            })
            .catch(err => console.log(err));

    }

    private getIndex(spiderUpdate: SpiderUpdate) {
        return `${this.elasticsearchIndex}-${spiderUpdate.spider.toLowerCase()}`;
    }

    async processFiles(index: string, spiderUpdate: SpiderUpdate, spiderFilesList: Array<SpiderFiles>): Promise<void> {
        if (spiderFilesList.length === 0) {
            return Promise.resolve();
        }
        const processingSpiderFiles: Array<Promise<void>> = [];
        for (let idx = 0; idx < this.parallel; idx++) {
            let spiderFiles = spiderFilesList.pop();
            if (spiderFiles === undefined) {
                break;
            }
            processingSpiderFiles.push(
                this.documentBuilder.build(spiderUpdate, spiderFiles)
                    .then(doc => this.upsert(index, spiderUpdate, doc)));
        }
        return Promise.all(processingSpiderFiles).then(_ => this.processFiles(index, spiderUpdate, spiderFilesList));
    }

    async upsert(index: string, spiderUpdate: SpiderUpdate, document: ELDocument): Promise<void> {
        const {deleted, ...data} = document;
        const id = document.id;
        if (deleted) {
            return Axios.delete(`${this.elasticsearchHost}/${index}/_doc/${id}`, {
                maxContentLength: Infinity,
                maxBodyLength: Infinity,
                auth: {
                    username: this.elasticsearchUser,
                    password: this.elasticsearchPassword
                }
            }).then(resp => {
                console.log(`deleting document ${id}`)
            }).catch(err => {
                if (err.response && err.response.data && err.response.data.error) {
                    throw { document: id, response: err.response.data.error }
                } else {
                    throw { document: id, response: err };
                }
            });
        } else {
            data.source = spiderUpdate.job;
            if (!document.hasOwnProperty('data')) {
                return Axios.post(`${this.elasticsearchHost}/${index}/_update/${id}`, {doc: data}, {
                    maxContentLength: Infinity,
                    maxBodyLength: Infinity,
                    auth: {
                        username: this.elasticsearchUser,
                        password: this.elasticsearchPassword
                    }
                }).then(resp => {
                    console.log(`processing document ${id}`)
                }).catch(err => {
                    if (err.response && err.response.data && err.response.data.error) {
                        throw { document: id, response: err.response.data.error }
                    } else {
                        throw { document: id, response: err };
                    }
                });
            } else {
                return Axios.put(`${this.elasticsearchHost}/${index}/_doc/${id}/?pipeline=attachment`, data, {
                    maxContentLength: Infinity,
                    maxBodyLength: Infinity,
                    auth: {
                        username: this.elasticsearchUser,
                        password: this.elasticsearchPassword
                    }
                }).then(resp => {
                    console.log(`processing document ${id}`)
                }).catch(err => {
                    if (err.response && err.response.data && err.response.data.error) {
                        throw { document: id, response: err.response.data.error }
                    } else {
                        throw { document: id, response: err };
                    }
                });
            }
        }
    }

    private static filterSpiderFiles(spiderDictionary: SpiderDictionary, spiderUpdate: SpiderUpdate): Array<SpiderFiles> {
        const spiderFilesList: Array<SpiderFiles> = [];

        let filePath: string = '';
        let spiderFiles: SpiderFiles = {};

        //sort fileNames
        const files: Array<string> = []
        for (let fileName in spiderUpdate.dateien) {
            if (spiderUpdate.dateien.hasOwnProperty(fileName)) {
                files.push(fileName)
            }
        }
        files.sort()

        for (let fileName of files) {
            const spiderFile = spiderUpdate.dateien[fileName];
            const existingSequence: number =
                spiderDictionary.hasOwnProperty(fileName) ? spiderDictionary[fileName] : -1;
            if (spiderFile.status === SpiderFileStatus.UPDATE || spiderFile.status === SpiderFileStatus.NEW ||
                (spiderFile.status === SpiderFileStatus.EQUAL &&
                    SpiderProcessor.getSequence(spiderFile.last_change) > existingSequence) ||
                (spiderFile.status === SpiderFileStatus.DELETED && existingSequence !== -1)) {
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
        if (Object.keys(spiderFiles).length > 0) {
            spiderFilesList.push(spiderFiles);
        }
        return spiderFilesList;
    }

    private static getSequence(job?: string): number {
        if (job === undefined || job === null) {
            return 0;
        }
        return parseInt(job.substring(job.lastIndexOf('/') + 1))
    }
}
