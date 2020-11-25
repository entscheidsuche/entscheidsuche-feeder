import fs from 'fs';
import path from 'path';
import * as stream from "stream";

export type FileLoader = {
    getStream(fileName:string): stream.Readable
}

export function fileLoader(): FileLoader {
    const loaderType = process.env.LOADER_TYPE;
    switch (loaderType) {
        case 'FILE': return new FSFileLoader(process.env.FILE_BASE_PATH!);
        default: throw new Error('unknown file loader ' + loaderType);
    }
}

class FSFileLoader implements FileLoader {

    constructor(private basePath: string) {}

    getStream(fileName: string): stream.Readable {
        return fs.createReadStream(path.join(this.basePath, fileName));
    }

}
