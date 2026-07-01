export enum SpiderFileStatus {
    EQUAL = "identisch",
    UPDATE = "update",
    NEW = "neu",
    DELETED = "nicht_mehr_da",
    // Konsolidierer-Status: Dokument war aus dem Index entfernt und ist wieder da.
    // Muss re-indexiert werden (identisch_wieder_da wie EQUAL, anders_wieder_da wie UPDATE).
    EQUAL_AGAIN = "identisch_wieder_da",
    CHANGED_AGAIN = "anders_wieder_da",
    // Inhaltsdatei fehlt auf dem Server -> ganzes Dokument auslassen (nicht bauen -> 404).
    BROKEN = "kaputt"
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
    deleted: boolean,
    source?: string,
    canton?: string,
    title?: IntText,
    abstract?: IntText,
    meta?: IntText,
    hierarchy?: Array<string>,
    reference?: Array<string>,
    date?: string,
    data?: string,
    url?: string,
    scrapedate?: string
}

