import Axios from "axios";
import https from "https";
import fs from "fs";

export class ElasticUtil {

    private elasticsearchHost: string;
    private elasticsearchUser: string;
    private elasticsearchPassword: string;

    private agent = new https.Agent({
        ca: fs.readFileSync(`${process.env.ELASTICSEARCH_CERT_PATH}`),
        rejectUnauthorized: true
    });

    constructor() {
        this.elasticsearchHost = `${process.env.ELASTICSEARCH_HOST}`;
        this.elasticsearchUser = `${process.env.ELASTICSEARCH_USER}`;
        this.elasticsearchPassword = `${process.env.ELASTICSEARCH_PASSWORD}`;
    }

    async existsDocument(id: string, index: string): Promise<boolean> {
        return Axios.head(`${this.elasticsearchHost}/${index}/_doc/${id}`, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            auth: {
                username: this.elasticsearchUser,
                password: this.elasticsearchPassword
            },
            httpsAgent: this.agent
        }).then(resp => {
            const exists = resp.status === 200;
            console.log(`document ${index}/${id}${exists ? '' : ' does not'} exist`);
            return exists;
        }).catch(err => {
            if (err.response && err.response.status) {
                const exists = err.response.status === 200;
                console.log(`document ${index}/${id}${exists ? '' : ' does not'} exist`);
                return exists;
            } else if (err.response && err.response.data && err.response.data.error) {
                throw {
                    index,
                    message: err.message,
                    code: err.code
                };
            } else {
                throw {
                    index,
                    message: err.message,
                    code: err.code
                };
            }
        });

    }

    // Returns the subset of ids that already exist in the index, with one _mget instead of one HEAD per id.
    async existingIds(index: string, ids: Array<string>): Promise<Set<string>> {
        if (ids.length === 0) {
            return new Set();
        }
        return Axios.post(`${this.elasticsearchHost}/${index}/_mget?_source=false`, {ids}, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            auth: {
                username: this.elasticsearchUser,
                password: this.elasticsearchPassword
            },
            httpsAgent: this.agent
        }).then(resp => {
            const docs: Array<any> = resp.data.docs ?? [];
            return new Set(docs.filter(doc => doc.found).map(doc => doc._id as string));
        }).catch(err => {
            throw {
                index,
                message: err.message,
                code: err.code,
                response: err.response?.data?.error
            };
        });
    }

    async bulkIndex(index: string, docs: Array<{id: string, body: any}>): Promise<void> {
        if (docs.length === 0) {
            return;
        }
        const ndjson = docs
            .map(doc => JSON.stringify({index: {_index: index, _id: doc.id}}) + '\n' + JSON.stringify(doc.body))
            .join('\n') + '\n';
        const resp = await Axios.post(`${this.elasticsearchHost}/_bulk`, ndjson, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            auth: {
                username: this.elasticsearchUser,
                password: this.elasticsearchPassword
            },
            headers: {
                'Content-Type': 'application/x-ndjson'
            },
            httpsAgent: this.agent
        }).catch(err => {
            throw {
                index,
                message: err.message,
                code: err.code,
                response: err.response?.data?.error
            };
        });
        if (resp.data.errors) {
            const failed = (resp.data.items as Array<any>)
                .map(item => item.index)
                .filter(result => result.error !== undefined)
                .map(result => ({id: result._id, error: result.error}));
            throw {index, message: `bulk indexing failed for ${failed.length} of ${docs.length} documents`, failed};
        }
        console.log(`bulk indexed ${docs.length} documents into ${index}`);
    }

    async existsIndex(index: string): Promise<boolean> {
        return Axios.head(`${this.elasticsearchHost}/${index}`, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            auth: {
                username: this.elasticsearchUser,
                password: this.elasticsearchPassword
            },
            httpsAgent: this.agent
        }).then(resp => {
            const exists = resp.status === 200;
            console.log(`index ${index}${exists ? '' : ' does not'} exist`);
            return exists;
        }).catch(err => {
            if (err.response && err.response.status) {
                const exists = err.response.status === 200;
                console.log(`index ${index}${exists ? '' : ' does not'} exist`);
                return exists;
            } else if (err.response && err.response.data && err.response.data.error) {
                throw {
                    index,
                    message: err.message,
                    code: err.code
                };
            } else {
                throw {
                    index,
                    message: err.message,
                    code: err.code
                };
            }
        });

    }

    async createIndex(index: string, mapping: Object): Promise<any> {
        return Axios.put(`${this.elasticsearchHost}/${index}`, mapping, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            auth: {
                username: this.elasticsearchUser,
                password: this.elasticsearchPassword
            },
            headers: {
                'Content-Type': 'application/json'
            },
            httpsAgent: this.agent
        }).then(resp => {
            console.log(`index ${index} created successfully`);
            return resp.data;
        }).catch(err => {
            if (err.response && err.response.data && err.response.data.error) {
                console.error(`failed to create index ${index}:`, err.response.data.error);
                throw {
                    index,
                    message: err.message,
                    code: err.code
                };
            } else {
                console.error(`failed to create index ${index}:`, err);
                throw {
                    index,
                    message: err.message,
                    code: err.code
                };
            }
        });
    }

    async updateIndex(index: string, mapping: Object): Promise<any> {
        return Axios.post(`${this.elasticsearchHost}/${index}/_mapping`, mapping, {
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            auth: {
                username: this.elasticsearchUser,
                password: this.elasticsearchPassword
            },
            headers: {
                'Content-Type': 'application/json'
            },
            httpsAgent: this.agent
        }).then(resp => {
            console.log(`index ${index} created successfully`);
            return resp.data;
        }).catch(err => {
            if (err.response && err.response.data && err.response.data.error) {
                console.error(`failed to create index ${index}:`, err.response.data.error);
                throw {
                    index,
                    message: err.message,
                    code: err.code
                };
            } else {
                console.error(`failed to create index ${index}:`, err);
                throw {
                    index,
                    message: err.message,
                    code: err.code
                };
            }
        });
    }
}