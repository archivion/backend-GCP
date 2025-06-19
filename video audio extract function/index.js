const { Storage } = require('@google-cloud/storage');
const path = require('path');
const os = require('os');
const fs = require('fs').promises;
const { spawn } = require('child_process');

const storage = new Storage();
const AUIDO_OUTPUT_BUCKET_NAME = process.env.OUTPUT_AUDIO_BUCKET_NAME;

exports.extractAudioHandler = async (message, context) => {
  if (!AUIDO_OUTPUT_BUCKET_NAME) {
    throw new Error('OUTPUT_AUDIO_BUCKET_NAME environment variable not set.');
  }

  const messageDataString = message.data ? Buffer.from(message.data, 'base64').toString() : null;
  if (!messageDataString) {
    console.error('Error: Pub/Sub message data is empty.');
    return; // Acknowledge message to prevent retries
  }
  console.log(`Received Pub/Sub message: ${messageDataString}`);

  let messagePayload;
  try {
    messagePayload = JSON.parse(messageDataString);
  } catch (e) {
    console.error('Error parsing Pub/Sub message data JSON:', e);
    return; // Acknowledge unparsable message
  }

  const { sourceBucketName, sourceFilePath, firestoreDocId } = messagePayload;
  if (!sourceBucketName || !sourceFilePath || !firestoreDocId) {
    console.error('Error: Missing required fields in Pub/Sub message.', messagePayload);
    return; // Acknowledge message with missing data
  }

  console.log(`Processing video: gs://<span class="math-inline">\{sourceBucketName\}/</span>{sourceFilePath} for Doc ID: ${firestoreDocId}`);

  const outputAudioFileName = `${firestoreDocId}.flac`;
  const tempLocalDir = os.tmpdir();
  const localVideoPath = path.join(tempLocalDir, path.basename(sourceFilePath));
  const localAudioPath = path.join(tempLocalDir, outputAudioFileName);

  try {
    console.log(`Downloading video to ${localVideoPath}...`);
    await storage.bucket(sourceBucketName).file(sourceFilePath).download({ destination: localVideoPath });
    console.log('Video downloaded.');

    console.log('Starting FFmpeg audio extraction...');
    await new Promise((resolve, reject) => {
      const ffmpegProcess = spawn('ffmpeg', ['-i', localVideoPath, '-y', '-vn', '-ac', '1', '-ar', '16000', '-acodec', 'flac', localAudioPath]);
      let ffmpegStderr = '';
      ffmpegProcess.stderr.on('data', (data) => { ffmpegStderr += data.toString(); });
      ffmpegProcess.on('error', (error) => reject(new Error(`FFmpeg failed to start: ${error.message}`)));
      ffmpegProcess.on('close', (code) => {
        if (code === 0) {
          console.log(`FFmpeg extraction successful.`);
          resolve();
        } else {
          console.error(`FFmpeg failed. Code: ${code}. Stderr: ${ffmpegStderr}`);
          reject(new Error(`FFmpeg process exited with code ${code}.`));
        }
      });
    });

    console.log(`Uploading extracted audio to gs://<span class="math-inline">\{AUIDO\_OUTPUT\_BUCKET\_NAME\}/</span>{outputAudioFileName}...`);
    await storage.bucket(AUIDO_OUTPUT_BUCKET_NAME).upload(localAudioPath, { destination: outputAudioFileName, metadata: { contentType: 'audio/flac' } });
    console.log('Audio successfully uploaded.');
  } catch (error) {
    console.error(`Error in extractAudioHandler for ${sourceFilePath}:`, error);
    throw error; // Rethrow to make Pub/Sub retry
  } finally {
    console.log('Cleaning up temporary files...');
    await fs.unlink(localVideoPath).catch(e => console.warn(`Failed to delete temp video file: ${e.message}`));
    const stats = await fs.stat(localAudioPath).catch(() => null);
    if (stats) await fs.unlink(localAudioPath).catch(e => console.warn(`Failed to delete temp audio file: ${e.message}`));
  }
  console.log(`Audio extraction process completed for ${sourceFilePath}.`);
};
