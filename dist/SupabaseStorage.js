"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.SupabaseStorage = void 0;
const drive_1 = require("@directus/drive");
const path_1 = __importDefault(require("path"));
const normalize_path_1 = __importDefault(require("normalize-path"));
const stream_1 = require("stream");
function handleError(err, path, bucket) {
    switch (err.name) {
        case 'NoSuchBucket':
            return new drive_1.NoSuchBucket(err, bucket);
        case 'NoSuchKey':
            return new drive_1.FileNotFound(err, path);
        case 'AllAccessDisabled':
            return new drive_1.PermissionMissing(err, path);
        default:
            return new drive_1.UnknownException(err, err.name, path);
    }
}
class SupabaseStorage extends drive_1.Storage {
    constructor(config) {
        super();
        const createClient = require('@supabase/supabase-js/dist/');
        this.$driver = createClient(config.url, config.secret);
        this.$folder = config.folder;
        this.$root = config.root ? (0, normalize_path_1.default)(config.root).replace(/^\//, '') : '';
        this.$acl = config.acl;
    }
    /**
     * Prefixes the given filePath with the storage root location
     */
    _fullPath(filePath) {
        return (0, normalize_path_1.default)(path_1.default.join(this.$root, filePath));
    }
    /**
     * Copy a file to a location.
     */
    async copy(src, dest) {
        try {
            const { data, error } = await this.$driver.storage.from(this.$folder).move(src, dest);
            if (error)
                throw new Error(error.message);
            return { raw: data };
        }
        catch (e) {
            throw handleError(e, src, this.$folder);
        }
    }
    /**
     * Delete existing file.
     */
    async delete(location) {
        try {
            const { data, error } = await this.$driver.storage.from(this.$folder).remove([location]);
            if (error)
                throw new Error(error.message);
            return { raw: data, wasDeleted: null };
        }
        catch (e) {
            throw handleError(e, location, this.$folder);
        }
    }
    /**
     * Returns the driver.
     */
    driver() {
        return this.$driver;
    }
    /**
     * Determines if a file or folder already exists.
     */
    async exists(location) {
        try {
            const { data, error } = await this.$driver.storage.from(this.$folder).download(location);
            if (error)
                throw new Error(error.message);
            return { exists: true, raw: data };
        }
        catch (e) {
            if (e.statusCode === 404) {
                return { exists: false, raw: e };
            }
            else {
                throw handleError(e, location, this.$folder);
            }
        }
    }
    /**
     * Returns the file contents.
     */
    async get(location, encoding = 'utf-8') {
        const bufferResult = await this.getBuffer(location);
        return {
            content: bufferResult.content.toString(encoding),
            raw: bufferResult.raw
        };
    }
    /**
     * Returns the file contents as Buffer.
     */
    async getBuffer(location) {
        try {
            const { data, error } = await this.$driver.storage.from(this.$folder).download(location);
            if (error)
                throw new Error(error.message);
            // S3.getObject returns a Buffer in Node.js
            const body = (await (data === null || data === void 0 ? void 0 : data.arrayBuffer()));
            return { content: body, raw: data };
        }
        catch (e) {
            throw handleError(e, location, this.$folder);
        }
    }
    /**
     * Returns signed url for an existing file
     */
    async getSignedUrl(location, options = {}) {
        try {
            const { data, error } = await this.$driver.storage.from(this.$folder).createSignedUrl(location, 900);
            if (error)
                throw new Error(error.message);
            return { signedUrl: (data === null || data === void 0 ? void 0 : data.signedURL) || '', raw: data };
        }
        catch (e) {
            throw handleError(e, location, this.$folder);
        }
    }
    /**
     * Returns file's size and modification date.
     */
    async getStat(location) {
        try {
            const { data, error } = await this.$driver.storage.from(this.$folder).download(location);
            if (error)
                throw new Error(error.message);
            const Blob = await (data === null || data === void 0 ? void 0 : data.arrayBuffer());
            return {
                size: Blob === null || Blob === void 0 ? void 0 : Blob.byteLength,
                modified: new Date(),
                raw: data
            };
        }
        catch (e) {
            throw handleError(e, location, this.$folder);
        }
    }
    /**
     * Returns the stream for the given file.
     */
    getStream(location, range) {
        const intermediateStream = new stream_1.PassThrough({ highWaterMark: 1 });
        try {
            this.$driver.storage
                .from(this.$folder)
                .download(location)
                .then(({ data, error }) => {
                if (error)
                    throw new Error(error.message);
                if (data)
                    return data.stream();
                return null;
            })
                .then((stream) => {
                if (!stream) {
                    throw handleError(new Error('Blobclient stream was not available'), location, this.$folder);
                }
                stream.pipe(intermediateStream);
            })
                .catch((error) => {
                intermediateStream.emit('error', error);
            });
        }
        catch (error) {
            intermediateStream.emit('error', error);
        }
        return intermediateStream;
    }
    /**
     * Returns url for a given key.
     */
    getUrl(location) {
        const { publicURL, error } = this.$driver.storage.from(this.$folder).getPublicUrl(location);
        if (error)
            throw new Error(error.message);
        return publicURL || '';
    }
    /**
     * Moves file from one location to another. This
     * method will call `copy` and `delete` under
     * the hood.
     */
    async move(src, dest) {
        src = this._fullPath(src);
        dest = this._fullPath(dest);
        await this.copy(src, dest);
        await this.delete(src);
        return { raw: undefined };
    }
    /**
     * Creates a new file.
     * This method will create missing directories on the fly.
     */
    async put(location, content
    // type?: string
    ) {
        try {
            const { data, error } = await this.$driver.storage.from(this.$folder).upload(location, content);
            if (error)
                throw new Error(error.message);
            return { raw: data };
        }
        catch (e) {
            throw handleError(e, location, this.$folder);
        }
    }
    /**
     * Iterate over all files in the bucket.
     */
    async *flatList(prefix = '') {
        prefix = this._fullPath(prefix);
        let continuationToken;
        do {
            try {
                const { data, error } = await this.$driver.storage.from(this.$folder).list(prefix);
                if (error)
                    throw new Error(error.message);
                if (data)
                    for (const file of data) {
                        const path = file.name;
                        yield {
                            raw: file,
                            path: path
                        };
                    }
            }
            catch (e) {
                throw handleError(e, prefix, this.$folder);
            }
        } while (continuationToken);
    }
}
exports.SupabaseStorage = SupabaseStorage;
