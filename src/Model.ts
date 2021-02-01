export enum SpiderFileStatus {
    EQUAL = "identisch",
    UPDATE = "update",
    NEW = "neu"
}

export type SpiderFile = {
    checksum: string,
    status: SpiderFileStatus,
    last_change?: string,
    quelle?: string
}

export type SpiderFiles = {
    [key in string]: SpiderFile
}

export type SpiderDictionary = {
    [key in string]: number
}

export type SpiderUpdate = {
    spider: string,
    job: string,
    jobtyp: string,
    time: string,
    dateien: SpiderFiles
}

export type IntText = {
    de: string,
    fr: string,
    it: string
}

export type ELDocument = {
    id: string,
    source?: string,
    canton: string,
    title?: IntText,
    abstract?: IntText,
    meta?: IntText,
    hierarchy: Array<string>,
    reference?: Array<string>,
    date?: string,
    data?: string,
    url?: string
}

