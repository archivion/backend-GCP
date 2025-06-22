// import needed GCP modules
const { ImageAnnotatorClient } = require('@google-cloud/vision');
const { VideoIntelligenceServiceClient } = require('@google-cloud/video-intelligence').v1;
const { SpeechClient } = require('@google-cloud/speech').v1p1beta1;
const { Firestore } = require('@google-cloud/firestore');
const { Storage } = require('@google-cloud/storage');
const { PubSub } = require('@google-cloud/pubsub');
const { VertexAI } = require('@google-cloud/vertexai');
// import mm lib for audio file
const mm = require('music-metadata');
// import node.js modules for file operations
const fs = require('fs').promises; // file system
const path = require('path'); // file directory 
const os = require('os'); //operating system -> temp. directory

//inisialisasi client
const visionClient = new ImageAnnotatorClient();
const videoClient = new VideoIntelligenceServiceClient();
const speechClient = new SpeechClient();
const pubSubClient = new PubSub();
const storage = new Storage();

const firestore = new Firestore({
  databaseId: process.env.DATABASE_ID
});

const vertex_ai = new VertexAI({
  project: 'elaborate-helix-461618-j3',
  location: 'us-central1',
});

const generativeModel = vertex_ai.getGenerativeModel({ model: 'gemini-2.0-flash-001' });

// generate topics with Vertex
async function generateTopicsFromTranscript(transcript) {
  const prompt = `You are an expert at analyzing text to find key topics.
  Based on the following transcript, identify the 5 to 7 most relevant topics or keywords.
  Rules for your response:
  - The topics should be concise, using 1 to 4 words each.
  - Your entire output MUST be a valid JSON array of strings and nothing else.
  Example of a perfect response: ["Artificial Intelligence", "Cloud Computing", "Startup Funding"]
  Transcript: """${transcript}"""
  JSON Output:`;
  const result = await generativeModel.generateContent({ contents: [{ role: 'user', parts: [{ text: prompt }] }] });
  // jika response kosong atau error
  const response = result.response;
  if (!response.candidates?.[0]?.content?.parts?.[0]?.text) {
      throw new Error('Gemini API returned an empty or invalid response.');
  }
  // extract content dan cleaning
  const responseText = response.candidates[0].content.parts[0].text;
  const jsonString = responseText.trim().replace(/```json/g, '').replace(/```/g, '');
  return JSON.parse(jsonString);
}
// set up untuk proses after processing
const METADATA_COLLECTION = process.env.METADATA_COLLECTION || 'media_metadata';
const AUDIO_BUCKET_NAME = process.env.EXTRACTED_AUDIO_BUCKET_NAME;
const audioExtractionTopicName = process.env.AUDIO_EXTRACTION_TOPIC;

function base64ToHex(str) {
  if (!str) return null;
  return Buffer.from(str, 'base64').toString('hex');
}
// function processMedia
exports.processMedia = async (event, context) => {
  const file = event;
  const filePath = file.name;
  const bucketName = file.bucket;
  const contentType = file.contentType || 'application/octet-stream';
  // URI for the media file
  const gcsUri = `gs://${bucketName}/${filePath}`;
  // jika file sudah dihapus, abort function
  if (file.resourceState === 'not_exists') {
    console.log(`File ${filePath} was deleted. Skipping.`);
    return;
  }
  // produce log file apa yang akan diproses dan jenisnya
  console.log(`Processing file: ${filePath} from bucket: ${bucketName}, Content-Type: ${contentType}`);
  // setting up fileID
  let firestoreDocId;
  // kalau dari bucket audio -> fileID sama dengan fileID dari video
  if (bucketName === AUDIO_BUCKET_NAME) {
    firestoreDocId = path.parse(filePath).name;
  } else { // kalau dari main bucket -> fileID hasil hash MD5 in hexadecimal
    firestoreDocId = base64ToHex(file.md5Hash);
    // kalau MD5 hash error -> fuction abort
    if (!firestoreDocId) {
        console.error(`Error: md5Hash is missing for file ${filePath}. Aborting.`);
        return;
    }
  }
  // produce log FileID
  console.log(`Using Firestore Document ID: ${firestoreDocId}`);
  // state doucument metadata yang akan dipakai
  const docRef = firestore.collection(METADATA_COLLECTION).doc(firestoreDocId);
  // media processing
  try {
    // audio processing path
    if (contentType.startsWith('audio/')) {
      // produce log confirm audio fileID
      console.log(`[AUDIO_PATH] Handling audio file for ID ${firestoreDocId}.`);
      // fetch kondisi metadata
      const existingDoc = await docRef.get();
      let metadataToSave; // metadata yang akan di save
      // cek apakah sudah ada file dengan metadata yang sama
      if (existingDoc.exists) {
        console.log(`[AUDIO_PATH] Found existing document. Will merge.`);
        metadataToSave = existingDoc.data();
      } else {
        console.log(`[AUDIO_PATH] No existing document found. Will create new.`);
        metadataToSave = {};
      }

      // update status and timestamp for the current run
      metadataToSave.processingStatus = 'In Progress';
      metadataToSave.processedAt = new Date().toISOString();
      // path untuk temporary file untuk download audio
      const tempLocalFile = path.join(os.tmpdir(), path.basename(filePath));
      
      try {
        await storage.bucket(bucketName).file(filePath).download({ destination: tempLocalFile });
        const audioFileMetadata = await mm.parseFile(tempLocalFile); // identifikasi metadata
        const { codec, sampleRate, numberOfChannels } = audioFileMetadata.format;
        let encoding;
        // encoding di tentukan berdasarkan jenis konten
        if (codec?.toLowerCase().includes('pcm') || contentType.includes('wav')) encoding = 'LINEAR16';
        else if (codec?.toLowerCase().includes('mpeg') || contentType.includes('mp3')) encoding = 'MP3';
        else if (codec?.toLowerCase().includes('flac') || contentType.includes('flac')) encoding = 'FLAC';
        else throw new Error(`Unsupported audio codec: ${codec}`);
        // pengaturan speecth yang akan di transcribe
        const speechConfig = { encoding, languageCode: 'en-US', enableAutomaticPunctuation: true, sampleRateHertz: sampleRate, audioChannelCount: numberOfChannels > 1 ? numberOfChannels : undefined };
        // inisiasi long running transcribe pada file sesuai pengaturan
        const [speechOperation] = await speechClient.longRunningRecognize({ audio: { uri: gcsUri }, config: speechConfig });
        const [speechResponse] = await speechOperation.promise(); // memastikan operasi selesai
        // cek apakah transcript ada
        if (speechResponse.results && speechResponse.results.length > 0) {
          const newTranscription = speechResponse.results.map(r => r.alternatives[0].transcript).join('\n').trim();
          metadataToSave.transcription = newTranscription; // menyimpan transcript di variabel metadataToSave
          if (newTranscription) {
            metadataToSave.speechApiStatus = 'success';
            // produce log kalau transcribe berhasil
            console.log(`[AUDIO_PATH] Transcription successful. Analyzing for topics...`);
            // memulai proses generate topics
            metadataToSave.topicsApiStatus = 'processing';
            try {
              metadataToSave.topics = await generateTopicsFromTranscript(newTranscription);
              metadataToSave.topicsApiStatus = 'success';
            } catch (geminiErr) { // jika terjadi error oleh gemini
              metadataToSave.topicsApiStatus = 'error';
              metadataToSave.topicsError = geminiErr.message;
              // produce log error
              console.error(`[AUDIO_PATH] Error during topic generation:`, geminiErr);
            }
          // update status speech API dan topics API
          } else { metadataToSave.speechApiStatus = 'no_transcription'; metadataToSave.topicsApiStatus = 'skipped_no_transcript'; }
        } else { metadataToSave.speechApiStatus = 'no_results'; metadataToSave.topicsApiStatus = 'skipped_no_transcript'; }
        
        metadataToSave.processingStatus = 'Completed'; // Set final status
      } catch (err) { // jika terjadi error
        metadataToSave.speechApiStatus = 'error';
        metadataToSave.speechError = err.message;
        metadataToSave.processingStatus = 'Failed'; // Set failed status
        console.error(`[AUDIO_PATH] Error during audio processing:`, err);
      } finally {
        if (await fs.stat(tempLocalFile).catch(() => false)) { // cek eksistensi file temporary
          // delete temporary audio file
          await fs.unlink(tempLocalFile).catch(e => console.warn(`Failed to delete temp audio file: ${e.message}`));
        }
      }

      // Conditionally update primary file info ONLY for direct uploads
      if (bucketName !== AUDIO_BUCKET_NAME) {
          metadataToSave.fileName = filePath;
          metadataToSave.bucket = bucketName;
          metadataToSave.contentType = contentType;
          metadataToSave.uploadTime = file.timeCreated;
          metadataToSave.mediaUri = gcsUri;
          metadataToSave.version = file.generation;
      }
      
      console.log(`[AUDIO_PATH] About to save audio analysis data to Firestore.`);
      // merge metadataToSave ke firestore
      await docRef.set(metadataToSave, { merge: true });
      console.log(`[AUDIO_PATH] Firestore merge complete for ID: ${firestoreDocId}.`);

    } else if (contentType.startsWith('video/')) {
      // --- VIDEO PROCESSING PATH ---
      console.log(`[VIDEO_PATH] Handling video for ID ${firestoreDocId}.`);
      let metadataToSave;
      const currentEventData = { fileName: filePath, bucket: bucketName, contentType: contentType, uploadTime: file.timeCreated, mediaUri: gcsUri, processedAt: new Date().toISOString(), version: file.generation };
      const existingDoc = await docRef.get();
      if (existingDoc.exists) { // cek apakah sudah ada dokumen ini di firetore
        // fetch data yang lama jika ada dan tambahkan data event sekarang
        metadataToSave = { ...existingDoc.data(), ...currentEventData, processingStatus: 'In Progress', videoApiStatus: 'pending', tags: existingDoc.data().tags || [], object_tags: existingDoc.data().object_tags || [], transcription: existingDoc.data().transcription || '', topics: existingDoc.data().topics || [], topicsApiStatus: existingDoc.data().topicsApiStatus || 'pending' };
      } else { // jika belum
        // inisialisasi objek metadata 
        metadataToSave = { ...currentEventData, tags: [], object_tags: [], transcription: '', processingStatus: 'In Progress', videoApiStatus: 'pending', speechApiStatus: 'pending', topics: [], topicsApiStatus: 'pending' };
      }
      metadataToSave.videoApiStatus = 'processing';
      // request vid intelligence API untuk labeling dan object tracking
      const [operation] = await videoClient.annotateVideo({ inputUri: gcsUri, features: ['LABEL_DETECTION', 'OBJECT_TRACKING'] });
      const [operationResult] = await operation.promise();
      const ar = operationResult.annotationResults?.[0];
      if (ar) { // jika terdapat hasil anotasi
        // memastikan bahwa tags bersifat unik / tidak berulang
        metadataToSave.tags = ar.segmentLabelAnnotations?.map(l => l.entity.description).filter(Boolean).filter((v, i, s) => s.indexOf(v) === i) || [];
        metadataToSave.object_tags = ar.objectAnnotations?.map(o => o.entity.description).filter(Boolean).filter((v, i, s) => s.indexOf(v) === i) || [];
        metadataToSave.videoApiStatus = (metadataToSave.tags.length > 0 || metadataToSave.object_tags.length > 0) ? 'success' : 'no_results';
      } else { metadataToSave.videoApiStatus = 'no_results'; }
      await docRef.set(metadataToSave, { merge: true }); // save metadata ke Firestore
      console.log(`[VIDEO_PATH] Visuals processing complete. Publishing message for audio extraction.`);
      // menyiapkan message untuk Pub/Sub
      const messageData = { sourceBucketName: bucketName, sourceFilePath: filePath, firestoreDocId: firestoreDocId };
      // Publish message ke Topic
      await pubSubClient.topic(audioExtractionTopicName).publishMessage({ data: Buffer.from(JSON.stringify(messageData)) });
      console.log(`[VIDEO_PATH] Message published to ${audioExtractionTopicName}.`);

    } else if (contentType.startsWith('image/')) {
      // --- IMAGE PROCESSING PATH ---
      console.log(`[IMAGE_PATH] Handling image for ID ${firestoreDocId}.`);
      let metadataToSave;
      const currentEventData = { fileName: filePath, bucket: bucketName, contentType: contentType, uploadTime: file.timeCreated, mediaUri: gcsUri, processedAt: new Date().toISOString(), version: file.generation };
      const existingDoc = await docRef.get();
      // fetch existing metadata jika ada
      if (existingDoc.exists) { metadataToSave = { ...existingDoc.data(), ...currentEventData, processingStatus: 'In Progress' }; } 
      else { metadataToSave = { ...currentEventData, tags: [], object_tags: [], processingStatus: 'In Progress' }; }
      metadataToSave.visionApiStatus = 'processing';
      // perform labeling dan object detection
      const [labelResult] = await visionClient.labelDetection(gcsUri);
      metadataToSave.tags = labelResult.labelAnnotations?.map(l => l.description).filter(Boolean) || [];
      const [objectResult] = await visionClient.objectLocalization(gcsUri);
      metadataToSave.object_tags = objectResult.localizedObjectAnnotations?.map(o => o.name).filter(Boolean) || [];
      // set status API dan pemrosesan
      metadataToSave.visionApiStatus = (metadataToSave.tags.length > 0 || metadataToSave.object_tags.length > 0) ? 'success' : 'no_results';
      metadataToSave.processingStatus = 'Completed';
      await docRef.set(metadataToSave, { merge: true });
      console.log(`[IMAGE_PATH] Image metadata save complete for ID: ${firestoreDocId}.`);

    } else { // jika tipe file bukan audio, video, atau image
      console.log(`Unsupported content type: ${contentType}. Skipping.`);
    }
  } catch (error) { // jika terdapat error di try block
    console.error(`FATAL ERROR for ${filePath} (ID: ${firestoreDocId}):`, error);
    // update status processing dengan status Failed
    await docRef.set({ processingError: { message: error.message, name: error.name }, processingStatus: 'Failed', processedAt: new Date().toISOString() }, { merge: true }).catch(e => console.error(`CRITICAL: Could not save fatal error state:`, e));
    throw error; // untuk retry atau monitoring
  }
};
