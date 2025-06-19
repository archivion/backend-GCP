const { ImageAnnotatorClient } = require('@google-cloud/vision');
const { VideoIntelligenceServiceClient } = require('@google-cloud/video-intelligence').v1;
const { SpeechClient } = require('@google-cloud/speech').v1p1beta1;
const { Firestore } = require('@google-cloud/firestore');
const { Storage } = require('@google-cloud/storage');
const { PubSub } = require('@google-cloud/pubsub');
const { VertexAI } = require('@google-cloud/vertexai');
const mm = require('music-metadata');
const fs = require('fs').promises;
const path = require('path');
const os = require('os');

const visionClient = new ImageAnnotatorClient();
const videoClient = new VideoIntelligenceServiceClient();
const speechClient = new SpeechClient();
const pubSubClient = new PubSub();
const storage = new Storage();

// Initialize clients using environment variables and explicit project/location
const firestore = new Firestore({
  databaseId: process.env.DATABASE_ID
});

const vertex_ai = new VertexAI({
  project: 'elaborate-helix-461618-j3',
  location: 'us-central1',
});

// Using a stable model identifier. Change if needed.
const generativeModel = vertex_ai.getGenerativeModel({ model: 'gemini-pro' });

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
  const response = result.response;
  if (!response.candidates?.[0]?.content?.parts?.[0]?.text) {
      throw new Error('Gemini API returned an empty or invalid response.');
  }
  const responseText = response.candidates[0].content.parts[0].text;
  const jsonString = responseText.trim().replace(/```json/g, '').replace(/```/g, '');
  return JSON.parse(jsonString);
}

const METADATA_COLLECTION = process.env.METADATA_COLLECTION || 'media_metadata';
const AUDIO_BUCKET_NAME = process.env.EXTRACTED_AUDIO_BUCKET_NAME;
const audioExtractionTopicName = process.env.AUDIO_EXTRACTION_TOPIC;

function base64ToHex(str) {
  if (!str) return null;
  return Buffer.from(str, 'base64').toString('hex');
}

exports.processMedia = async (event, context) => {
  const file = event;
  const filePath = file.name;
  const bucketName = file.bucket;
  const contentType = file.contentType || 'application/octet-stream';
  const gcsUri = `gs://${bucketName}/${filePath}`;

  if (file.resourceState === 'not_exists') {
    console.log(`File ${filePath} was deleted. Skipping.`);
    return;
  }
  console.log(`Processing file: ${filePath} from bucket: ${bucketName}, Content-Type: ${contentType}`);
  
  let firestoreDocId;
  if (bucketName === AUDIO_BUCKET_NAME) {
    firestoreDocId = path.parse(filePath).name;
  } else {
    firestoreDocId = base64ToHex(file.md5Hash);
    if (!firestoreDocId) {
        console.error(`Error: md5Hash is missing for file ${filePath}. Aborting.`);
        return;
    }
  }
  console.log(`Using Firestore Document ID: ${firestoreDocId}`);

  const docRef = firestore.collection(METADATA_COLLECTION).doc(firestoreDocId);

  try {
    if (contentType.startsWith('audio/')) {
      console.log(`[AUDIO_PATH] Handling audio file for ID ${firestoreDocId}.`);
      let audioUpdateData = {
        transcription: '', topics: [],
        speechApiStatus: 'processing', topicsApiStatus: 'pending',
        processedAt: new Date().toISOString(),
        speechError: null, topicsError: null,
      };
      const tempLocalFile = path.join(os.tmpdir(), path.basename(filePath));
      try {
        await storage.bucket(bucketName).file(filePath).download({ destination: tempLocalFile });
        const audioFileMetadata = await mm.parseFile(tempLocalFile);
        const { codec, sampleRate, numberOfChannels } = audioFileMetadata.format;
        let encoding;
        if (codec?.toLowerCase().includes('pcm') || contentType.includes('wav')) encoding = 'LINEAR16';
        else if (codec?.toLowerCase().includes('mpeg') || contentType.includes('mp3')) encoding = 'MP3';
        else if (codec?.toLowerCase().includes('flac') || contentType.includes('flac')) encoding = 'FLAC';
        else throw new Error(`Unsupported audio codec: ${codec}`);
        const speechConfig = { encoding, languageCode: 'en-US', enableAutomaticPunctuation: true, sampleRateHertz: sampleRate, audioChannelCount: numberOfChannels > 1 ? numberOfChannels : undefined };
        const [speechOperation] = await speechClient.longRunningRecognize({ audio: { uri: gcsUri }, config: speechConfig });
        const [speechResponse] = await speechOperation.promise();
        if (speechResponse.results && speechResponse.results.length > 0) {
          const newTranscription = speechResponse.results.map(r => r.alternatives[0].transcript).join('\n').trim();
          audioUpdateData.transcription = newTranscription;
          if (newTranscription) {
            audioUpdateData.speechApiStatus = 'success';
            console.log(`[AUDIO_PATH] Transcription successful. Analyzing for topics...`);
            audioUpdateData.topicsApiStatus = 'processing';
            try {
              audioUpdateData.topics = await generateTopicsFromTranscript(newTranscription);
              audioUpdateData.topicsApiStatus = 'success';
              console.log(`[AUDIO_PATH] Topics generated:`, audioUpdateData.topics);
            } catch (geminiErr) {
              audioUpdateData.topicsApiStatus = 'error';
              audioUpdateData.topicsError = geminiErr.message;
              console.error(`[AUDIO_PATH] Error during topic generation:`, geminiErr);
            }
          } else { audioUpdateData.speechApiStatus = 'no_transcription'; audioUpdateData.topicsApiStatus = 'skipped_no_transcript'; }
        } else { audioUpdateData.speechApiStatus = 'no_results'; audioUpdateData.topicsApiStatus = 'skipped_no_transcript'; }
      } catch (err) {
        audioUpdateData.speechApiStatus = 'error';
        audioUpdateData.speechError = err.message;
        console.error(`[AUDIO_PATH] Error during audio processing:`, err);
      } finally {
        if (await fs.stat(tempLocalFile).catch(() => false)) {
          await fs.unlink(tempLocalFile).catch(e => console.warn(`Failed to delete temp audio file: ${e.message}`));
        }
      }

      // --- MODIFICATION START ---
      let finalDataToMerge = audioUpdateData;

      // If this audio was a DIRECT upload (not from the extracted audio bucket),
      // then we also want to save its own file metadata.
      if (bucketName !== AUDIO_BUCKET_NAME) {
          const fileInfo = {
            fileName: filePath, bucket: bucketName, contentType: contentType,
            uploadTime: file.timeCreated, mediaUri: gcsUri, version: file.generation,
          };
          finalDataToMerge = { ...fileInfo, ...audioUpdateData };
      }
      // If the audio was from the extracted audio bucket, we ONLY merge audioUpdateData,
      // which preserves the original video's fileName and other metadata.
      // --- MODIFICATION END ---
      
      console.log(`[AUDIO_PATH] About to merge audio analysis data to Firestore. Data:`, JSON.stringify(finalDataToMerge, null, 2));
      await docRef.set(finalDataToMerge, { merge: true });
      console.log(`[AUDIO_PATH] Firestore merge complete for ID: ${firestoreDocId}.`);

    } else if (contentType.startsWith('video/')) {
      // ... Video processing logic remains the same ...
      console.log(`[VIDEO_PATH] Handling video for ID ${firestoreDocId}.`);
      let metadataToSave;
      const currentEventData = { fileName: filePath, bucket: bucketName, contentType: contentType, uploadTime: file.timeCreated, mediaUri: gcsUri, processedAt: new Date().toISOString(), version: file.generation };
      const existingDoc = await docRef.get();
      if (existingDoc.exists) {
        metadataToSave = { ...existingDoc.data(), ...currentEventData, videoApiStatus: 'pending', tags: existingDoc.data().tags || [], object_tags: existingDoc.data().object_tags || [], transcription: existingDoc.data().transcription || '', topics: existingDoc.data().topics || [], topicsApiStatus: existingDoc.data().topicsApiStatus || 'pending' };
      } else {
        metadataToSave = { ...currentEventData, tags: [], object_tags: [], transcription: '', videoApiStatus: 'pending', speechApiStatus: 'pending', topics: [], topicsApiStatus: 'pending' };
      }
      metadataToSave.videoApiStatus = 'processing';
      const [operation] = await videoClient.annotateVideo({ inputUri: gcsUri, features: ['LABEL_DETECTION', 'OBJECT_TRACKING'] });
      const [operationResult] = await operation.promise();
      const ar = operationResult.annotationResults?.[0];
      if (ar) {
        metadataToSave.tags = ar.segmentLabelAnnotations?.map(l => l.entity.description).filter(Boolean).filter((v, i, s) => s.indexOf(v) === i) || [];
        metadataToSave.object_tags = ar.objectAnnotations?.map(o => o.entity.description).filter(Boolean).filter((v, i, s) => s.indexOf(v) === i) || [];
        metadataToSave.videoApiStatus = (metadataToSave.tags.length > 0 || metadataToSave.object_tags.length > 0) ? 'success' : 'no_results';
      } else { metadataToSave.videoApiStatus = 'no_results'; }
      console.log(`[VIDEO_PATH] Visuals processing complete. Saving metadata and publishing message.`);
      await docRef.set(metadataToSave, { merge: true });
      const messageData = { sourceBucketName: bucketName, sourceFilePath: filePath, firestoreDocId: firestoreDocId };
      await pubSubClient.topic(audioExtractionTopicName).publishMessage({ data: Buffer.from(JSON.stringify(messageData)) });
      console.log(`[VIDEO_PATH] Message published to ${audioExtractionTopicName}.`);
    } else if (contentType.startsWith('image/')) {
      // ... Image processing logic remains the same ...
      console.log(`[IMAGE_PATH] Handling image for ID ${firestoreDocId}.`);
      let metadataToSave;
      const currentEventData = { fileName: filePath, bucket: bucketName, contentType: contentType, uploadTime: file.timeCreated, mediaUri: gcsUri, processedAt: new Date().toISOString(), version: file.generation };
      const existingDoc = await docRef.get();
      if (existingDoc.exists) { metadataToSave = { ...existingDoc.data(), ...currentEventData }; } 
      else { metadataToSave = { ...currentEventData, tags: [], object_tags: [] }; }
      metadataToSave.visionApiStatus = 'processing';
      const [labelResult] = await visionClient.labelDetection(gcsUri);
      metadataToSave.tags = labelResult.labelAnnotations?.map(l => l.description).filter(Boolean) || [];
      const [objectResult] = await visionClient.objectLocalization(gcsUri);
      metadataToSave.object_tags = objectResult.localizedObjectAnnotations?.map(o => o.name).filter(Boolean) || [];
      metadataToSave.visionApiStatus = (metadataToSave.tags.length > 0 || metadataToSave.object_tags.length > 0) ? 'success' : 'no_results';
      await docRef.set(metadataToSave, { merge: true });
      console.log(`[IMAGE_PATH] Image metadata save complete.`);
    } else {
      console.log(`Unsupported content type: ${contentType}. Skipping.`);
    }
  } catch (error) {
    console.error(`FATAL ERROR for ${filePath} (ID: ${firestoreDocId}):`, error);
    await docRef.set({ processingError: { message: error.message, name: error.name }, processedAt: new Date().toISOString() }, { merge: true }).catch(e => console.error(`CRITICAL: Could not save fatal error state:`, e));
    throw error;
  }
};