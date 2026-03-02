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