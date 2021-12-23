import { SupabaseClient } from '@supabase/supabase-js';
import {
	Storage,
	UnknownException,
	NoSuchBucket,
	FileNotFound,
	PermissionMissing,
	SignedUrlOptions,
	Response,
	ExistsResponse,
	ContentResponse,
	SignedUrlResponse,
	StatResponse,
	FileListResponse,
	DeleteResponse,
	Range
} from '@directus/drive';
import path from 'path';
import normalize from 'normalize-path';
import { PassThrough } from 'stream';

function handleError(err: Error, path: string, bucket: string): Error {
	switch (err.name) {
		case 'NoSuchBucket':
			return new NoSuchBucket(err, bucket);
		case 'NoSuchKey':
			return new FileNotFound(err, path);
		case 'AllAccessDisabled':
			return new PermissionMissing(err, path);
		default:
			return new UnknownException(err, err.name, path);
	}
}

export class SupabaseStorage extends Storage {
	protected $driver: SupabaseClient;
	protected $folder: string;
	protected $root: string;
	protected $acl?: string;

	constructor(config: SupabaseStorageConfig) {
		super();
		const createClient = require('@supabase/supabase-js/dist/');

		this.$driver = createClient(config.url, config.secret);

		this.$folder = config.folder;
		this.$root = config.root ? normalize(config.root).replace(/^\//, '') : '';
		this.$acl = config.acl;
	}

	/**
	 * Prefixes the given filePath with the storage root location
	 */
	protected _fullPath(filePath: string): string {
		return normalize(path.join(this.$root, filePath));
	}

	/**
	 * Copy a file to a location.
	 */
	public async copy(src: string, dest: string): Promise<Response> {
		try {
			const { data, error } = await this.$driver.storage.from(this.$folder).move(src, dest);
			if (error) throw new Error(error.message);
			return { raw: data };
		} catch (e: any) {
			throw handleError(e, src, this.$folder);
		}
	}

	/**
	 * Delete existing file.
	 */
	public async delete(location: string): Promise<DeleteResponse> {
		try {
			const { data, error } = await this.$driver.storage.from(this.$folder).remove([location]);
			if (error) throw new Error(error.message);
			return { raw: data, wasDeleted: null };
		} catch (e: any) {
			throw handleError(e, location, this.$folder);
		}
	}

	/**
	 * Returns the driver.
	 */
	public driver(): SupabaseClient {
		return this.$driver;
	}

	/**
	 * Determines if a file or folder already exists.
	 */
	public async exists(location: string): Promise<ExistsResponse> {
		try {
			const { data, error } = await this.$driver.storage.from(this.$folder).download(location);
			if (error) throw new Error(error.message);
			return { exists: true, raw: data };
		} catch (e: any) {
			if (e.statusCode === 404) {
				return { exists: false, raw: e };
			} else {
				throw handleError(e, location, this.$folder);
			}
		}
	}

	/**
	 * Returns the file contents.
	 */
	public async get(location: string, encoding: BufferEncoding = 'utf-8'): Promise<ContentResponse<string>> {
		const bufferResult = await this.getBuffer(location);

		return {
			content: bufferResult.content.toString(encoding),
			raw: bufferResult.raw
		};
	}

	/**
	 * Returns the file contents as Buffer.
	 */
	public async getBuffer(location: string): Promise<ContentResponse<Buffer>> {
		try {
			const { data, error } = await this.$driver.storage.from(this.$folder).download(location);
			if (error) throw new Error(error.message);

			// S3.getObject returns a Buffer in Node.js
			const body = (await data?.arrayBuffer()) as Buffer;

			return { content: body, raw: data };
		} catch (e: any) {
			throw handleError(e, location, this.$folder);
		}
	}

	/**
	 * Returns signed url for an existing file
	 */
	public async getSignedUrl(location: string, options: SignedUrlOptions = {}): Promise<SignedUrlResponse> {
		try {
			const { data, error } = await this.$driver.storage.from(this.$folder).createSignedUrl(location, 900);
			if (error) throw new Error(error.message);
			return { signedUrl: data?.signedURL || '', raw: data };
		} catch (e: any) {
			throw handleError(e, location, this.$folder);
		}
	}

	/**
	 * Returns file's size and modification date.
	 */
	public async getStat(location: string): Promise<StatResponse> {
		try {
			const { data, error } = await this.$driver.storage.from(this.$folder).download(location);
			if (error) throw new Error(error.message);
			const Blob = await data?.arrayBuffer();
			return {
				size: Blob?.byteLength as number,
				modified: new Date(),
				raw: data
			};
		} catch (e: any) {
			throw handleError(e, location, this.$folder);
		}
	}

	/**
	 * Returns the stream for the given file.
	 */
	public getStream(location: string, range?: Range): NodeJS.ReadableStream {
		const intermediateStream = new PassThrough({ highWaterMark: 1 });

		try {
			this.$driver.storage
				.from(this.$folder)
				.download(location)
				.then(({ data, error }) => {
					if (error) throw new Error(error.message);
					if (data) return data.stream();
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
		} catch (error: any) {
			intermediateStream.emit('error', error);
		}

		return intermediateStream;
	}

	/**
	 * Returns url for a given key.
	 */
	public getUrl(location: string): string {
		const { publicURL, error } = this.$driver.storage.from(this.$folder).getPublicUrl(location);
		if (error) throw new Error(error.message);
		return publicURL || '';
	}

	/**
	 * Moves file from one location to another. This
	 * method will call `copy` and `delete` under
	 * the hood.
	 */
	public async move(src: string, dest: string): Promise<Response> {
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
	public async put(
		location: string,
		content: Buffer | NodeJS.ReadableStream | string
		// type?: string
	): Promise<Response> {
		try {
			const { data, error } = await this.$driver.storage.from(this.$folder).upload(location, content);
			if (error) throw new Error(error.message);
			return { raw: data };
		} catch (e: any) {
			throw handleError(e, location, this.$folder);
		}
	}

	/**
	 * Iterate over all files in the bucket.
	 */
	public async *flatList(prefix = ''): AsyncIterable<FileListResponse> {
		prefix = this._fullPath(prefix);

		let continuationToken: string | undefined;

		do {
			try {
				const { data, error } = await this.$driver.storage.from(this.$folder).list(prefix);
				if (error) throw new Error(error.message);
				if (data)
					for (const file of data) {
						const path = file.name as string;

						yield {
							raw: file,
							path: path
						};
					}
			} catch (e: any) {
				throw handleError(e, prefix, this.$folder);
			}
		} while (continuationToken);
	}
}

export interface SupabaseStorageConfig {
	url: string;
	secret: string;
	folder: string;
	root?: string;
	acl?: string;
}
