export enum SpiderFileStatus {
    EQUAL = "identisch",
    NEW = "neu",
    UPDATE = "update"
}

export type SpiderFile = {
    checksum: string,
    status: SpiderFileStatus,
    quelle?: string
}

export type SpiderFiles = {
    [key in string]: SpiderFile
}

export type SpiderUpdate = {
    spider: string,
    job: string,
    jobtyp: string,
    time: string,
    dateien: SpiderFiles
}

export type ELDocument = {
    id: string,
    titel: string,
    leitsatz?: string,
    rechtsgebiet: string,
    hierarchie: Array<string>,
    signatur: string,
    kanton: string,
    gericht: string,
    geschaeftsnummer: string,
    edatum?: string,
    data?: string
}

