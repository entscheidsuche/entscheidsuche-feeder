import fs from 'fs';
import { get } from 'https';
import path from 'path';
import * as stream from "stream";
import S3 from 'aws-sdk/clients/s3';
import * as Stream from 'stream'

export type FileLoader = {
    getStream(fileName:string): stream.Readable
}

export function fileLoader(): FileLoader {
    const loaderType = process.env.LOADER_TYPE;
    switch (loaderType) {
        case 'FILE': return new FSFileLoader(process.env.FILE_BASE_PATH!);
        case 'S3': return new S3FileLoader();
        case 'HTTPS': return new HTTPSFileLoader(process.env.DOCUMENT_BASE_URL!);
        default: throw new Error('unknown file loader ' + loaderType);
    }
}

class FSFileLoader implements FileLoader {

    constructor(private basePath: string) {}

    getStream(fileName: string): stream.Readable {
        return fs.createReadStream(path.join(this.basePath, fileName));
    }

}

class HTTPSFileLoader implements FileLoader {

    constructor(private basePath: string) {}

    getStream(fileName: string): stream.Readable {
        const readableStream = new Stream.Transform({
                transform(chunk: any, encoding: BufferEncoding, callback: Stream.TransformCallback) {
                    callback(null, chunk);
            }});
        const request = get(`${this.basePath}/${fileName}`, function(message) {
            if (message.statusCode !== undefined && message.statusCode >= 400) {
                readableStream.emit('error', new Error(`${message.statusCode}: ${message.statusMessage}`))
            } else {
                message.pipe(readableStream);
            }
        });
        return readableStream;
    }
}

class S3FileLoader implements FileLoader {

    private s3:S3;

    constructor() {
        this.s3 = new S3({
            region: process.env.AWS_REGION,
            credentials: {
                accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
                secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!
            }});
    }

    getStream(fileName: string): stream.Readable {
        return this.s3.getObject({
            Bucket: 'entscheidsuche.ch',
            Key: `scraper/${fileName}`
        }).createReadStream();
    }

}
