import Bigint from '../tools/bigint'
import Bluebird from 'bluebird'
import {Readable} from 'stream'
import {request} from '../tools/smb2-forge'

const requestAsync = Bluebird.promisify(request)

const maxPacketSize = 0x00010000

class SmbReadableStream extends Readable {
  constructor (connection, file, options = {}) {
    super(options)

    const {
      start = 0,
      end,
      encoding
    } = options

    this.connection = connection
    this.encoding = encoding
    this.file = file
    this.offset = new Bigint(8, start)

    let fileLength = 0
    for (let i = 0; i < file.EndofFile.length; i++) {
      fileLength |= file.EndofFile[i] << (i * 8)
    }
    this.fileLength = fileLength
    this.wait = false

    if (end >= 0 && end < fileLength) {
      this.fileLength = end + 1
    }

    this._queryResultBuffer = [];
    this._queryReadIndex = 0;

    let temporaryIndex = 0;
    while(this.offset.lt(this.fileLength)) {
      const rest = this.offset.sub(this.fileLength).neg();
      const packetSize = Math.min(maxPacketSize, rest.toNumber());

      const offset = new Bigint(this.offset);

      this._queryResultBuffer[temporaryIndex] = null;
      requestAsync('read', {
        FileId: this.file.FileId,
        Length: packetSize,
        Offset: offset.toBuffer()
      }, this.connection).then((result) => {
        this._queryResultBuffer[temporaryIndex] = result;
      });

      this.offset = this.offset.add(packetSize);
      temporaryIndex = temporaryIndex + 1;
    }

    this._queryLength = temporaryIndex;
  }

  async _read (size) {
    while (this.queryReadIndex < this._lastQueryIndex) {
      if (this.wait) {
        return;
      }

      if (this._queryResultBuffer[this._queryReadIndex] === null) { // waiting for this query to come back
        return;
      }

      if (!this.push(content)) {
        return
      }

      this._queryReadIndex = this._queryReadIndex + 1;
    }

    if (this.queryReadIndex > this._lastQueryIndex) {
      this.push(null)
      await requestAsync('close', this.file, this.connection)
    }
  }
}

export default function (path, options, cb) {
  if (typeof options === 'function') {
    cb = options
    options = {}
  }
  request('open', {path}, this, (err, file) => {
    if (err) {
      if (err.code === 'STATUS_OBJECT_NAME_NOT_FOUND') {
        err.code = 'ENOENT'
      }
      cb(err)
    } else {
      cb(null, new SmbReadableStream(this, file, options))
    }
  })
}
