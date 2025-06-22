// import client library
const { Storage } = require('@google-cloud/storage');
// import node.js module
const path = require('path');
const os = require('os');
const fs = require('fs').promises;
// import spawn function from node.js karena ada external program FFmpeg
const { spawn } = require('child_process');

const storage = new Storage();
// inisialisasi buket output audio
const AUIDO_OUTPUT_BUCKET_NAME = process.env.OUTPUT_AUDIO_BUCKET_NAME;

// extract audio function
exports.extractAudioHandler = async (message, context) => {
  // cek apakah buket output sudah ada
  if (!AUIDO_OUTPUT_BUCKET_NAME) {
    throw new Error('OUTPUT_AUDIO_BUCKET_NAME environment variable not set.');
  }
  // decode Pub/Sub message
  const messageDataString = message.data ? Buffer.from(message.data, 'base64').toString() : null;
  // cek jika message kosong
  if (!messageDataString) {
    console.error('Error: Pub/Sub message data is empty.');
    return; // Acknowledge message to prevent retries
  }
  // produce log terkait message yang diterima
  console.log(`Received Pub/Sub message: ${messageDataString}`);

  let messagePayload; // inisialisasi untuk simpan message yang udah terpisah
  try {
    messagePayload = JSON.parse(messageDataString);
  } catch (e) { // jika gagal diurai
    console.error('Error parsing Pub/Sub message data JSON:', e);
    return; // Acknowledge unparsable message
  }
  // inisialisasi berdasarkan message yang sudah diurai
  const { sourceBucketName, sourceFilePath, firestoreDocId } = messagePayload;
  // cek jika ada detail yang gaada
  if (!sourceBucketName || !sourceFilePath || !firestoreDocId) {
    console.error('Error: Missing required fields in Pub/Sub message.', messagePayload);
    return; // Acknowledge message with missing data
  }
  // produce log mengenai video yang akan diproses
  console.log(`Processing video: gs://<span class="math-inline">\{sourceBucketName\}/</span>{sourceFilePath} for Doc ID: ${firestoreDocId}`);

  const outputAudioFileName = `${firestoreDocId}.flac`; // nama audio file yang akan dihasilkan
  // inisialisasi temporary file
  const tempLocalDir = os.tmpdir();
  const localVideoPath = path.join(tempLocalDir, path.basename(sourceFilePath));
  const localAudioPath = path.join(tempLocalDir, outputAudioFileName);
  // proses ekstraksi
  try {
    // download video ke temporary storage
    console.log(`Downloading video to ${localVideoPath}...`);
    await storage.bucket(sourceBucketName).file(sourceFilePath).download({ destination: localVideoPath });
    console.log('Video downloaded.');
    // memulai proses ekstraksi
    console.log('Starting FFmpeg audio extraction...');
    await new Promise((resolve, reject) => {
      const ffmpegProcess = spawn('ffmpeg', ['-i', localVideoPath, '-y', '-vn', '-ac', '1', '-ar', '16000', '-acodec', 'flac', localAudioPath]);
      // cek jika ada error
      let ffmpegStderr = '';
      ffmpegProcess.stderr.on('data', (data) => { ffmpegStderr += data.toString(); });
      ffmpegProcess.on('error', (error) => reject(new Error(`FFmpeg failed to start: ${error.message}`)));
      ffmpegProcess.on('close', (code) => {
        // cek jika berhasil jalan
        if (code === 0) {
          console.log(`FFmpeg extraction successful.`);
          resolve();
        } else { // jika ekstraksi gaga;
          console.error(`FFmpeg failed. Code: ${code}. Stderr: ${ffmpegStderr}`);
          reject(new Error(`FFmpeg process exited with code ${code}.`));
        }
      });
    });
    // upload hasil audio ke bucket audio output
    console.log(`Uploading extracted audio to gs://<span class="math-inline">\{AUIDO\_OUTPUT\_BUCKET\_NAME\}/</span>{outputAudioFileName}...`);
    await storage.bucket(AUIDO_OUTPUT_BUCKET_NAME).upload(localAudioPath, { destination: outputAudioFileName, metadata: { contentType: 'audio/flac' } });
    console.log('Audio successfully uploaded.');
  } catch (error) { // jika ada error
    console.error(`Error in extractAudioHandler for ${sourceFilePath}:`, error);
    throw error; // Rethrow to make Pub/Sub retry
  } finally { // proses menghapus temporary files
    console.log('Cleaning up temporary files...');
    await fs.unlink(localVideoPath).catch(e => console.warn(`Failed to delete temp video file: ${e.message}`));
    const stats = await fs.stat(localAudioPath).catch(() => null);
    if (stats) await fs.unlink(localAudioPath).catch(e => console.warn(`Failed to delete temp audio file: ${e.message}`));
  }
  // produce log setelah ekstraksi selesai
  console.log(`Audio extraction process completed for ${sourceFilePath}.`);
};
