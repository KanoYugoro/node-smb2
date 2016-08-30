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

    this._queryInformationBuffer = [];
    this._queryResultBuffer = [];
    this._queryReadIndex = 0;
    this._queryFinishedIndex = 0;
    this._activeQueries = 0;
    this._maxQueries = 2;

    let temporaryIndex = 0;

    while(this.offset.lt(this.fileLength)) {
        const rest = this.offset.sub(this.fileLength).neg();
        const packetSize = Math.min(maxPacketSize, rest.toNumber());
        const offset = new Bigint(this.offset);

        this._queryInformationBuffer[temporaryIndex] = {
          FileId: this.file.FileId,
          Length: packetSize,
          Offset: offset.toBuffer()
        };

        this.offset = this.offset.add(packetSize);
        temporaryIndex = temporaryIndex + 1;
    }
    this._queryLength = temporaryIndex;
    // console.log('done constructing');
    // console.log(this._queryLength);
  }

  async _read (size) {
    while (this._queryFinishedIndex < this._queryLength) {
      // console.log(`${this._queryReadIndex} ${this._queryLength} ${this._queryReadIndex} ${this._queryFinishedIndex} ${this._maxQueries}`);
      if (this._queryResultBuffer[this._queryFinishedIndex]) {
        // console.log(`Attempting to push out ${this._queryFinishedIndex}`);

        const res = this.push(this._queryResultBuffer[this._queryFinishedIndex], 'utf8');
        // console.log(`Result: ${res}, ${this._queryFinishedIndex}`);
        this._queryFinishedIndex = this._queryFinishedIndex + 1;
      }

      this.wait = this._activeQueries >= this._maxQueries;

      if (this.wait) {
        // console.log('waiting');
        return
      }

      // console.log(`Starting ${this._queryReadIndex}`);
      const iteration = this._queryReadIndex;
      this._activeQueries = this._activeQueries + 1;
      let content = await requestAsync('read', this._queryInformationBuffer[iteration], this.connection)
        .then(result => {
          // console.log(`${iteration} - came back`);
          this._activeQueries = this._activeQueries - 1;
          return result;
        });

      this._queryReadIndex = this._queryReadIndex + 1;
      if (this.encoding) {
        content = content.toString(this.encoding);
      }

      // console.log(`Putting ${this._queryReadIndex} in buffer`);
      this._queryResultBuffer[iteration] = content;
    }

    if (this._queryFinishedIndex >= this._queryLength) {
      // console.log('complete');
      this.push(null);
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
