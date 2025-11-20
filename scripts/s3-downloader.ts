/**
 * @fileoverview Script for downloading call recording files from an S3 bucket.
 * @author Szepetry
 *
 * This script downloads audio files from specified folders in an S3 bucket
 *
 * Usage:
 *   yarn exec tsx scripts/s3-downloader.ts
 *   npx tsx scripts/s3-downloader.ts
 */

import dotenv from "dotenv";
import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { createWriteStream } from "fs";
import { mkdir } from "fs/promises";
import { dirname, join } from "path";
import { pipeline } from "stream/promises";

dotenv.config();

const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY;
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID;
const AWS_REGION = process.env.AWS_REGION || "us-east-1";
const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME || "daily-recordings-raw";
const OUTPUT_DIR = "liam-recordings";

// Validate required environment variables
if (!AWS_ACCESS_KEY_ID || !AWS_SECRET_ACCESS_KEY) {
  console.error("Error: AWS credentials are required.");
  process.exit(1);
}

// ! add rooms to download, or leave empty to download all rooms
const neonDemos = [
  "A60",
  "A61",
  "A62",
  "A63",
  "A64",
  "A65",
  "A66",
  "A67",
  "A68",
  "A69",
  "Neon-Demo-1763675025826",
];

const FOLDER_PREFIX = "neonmobile";
const DEFAULT_AUDIO_EXTENSION = ".webm";

// Initialize S3 client
const s3Client = new S3Client({
  region: AWS_REGION,
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
  },
});

/**
 * Download a file from S3
 */
async function downloadFile(key: string, localPath: string): Promise<void> {
  try {
    // Ensure the directory exists
    await mkdir(dirname(localPath), { recursive: true });

    const command = new GetObjectCommand({
      Bucket: S3_BUCKET_NAME,
      Key: key,
    });

    const response = await s3Client.send(command);

    if (response.Body) {
      const writeStream = createWriteStream(localPath);
      await pipeline(response.Body as NodeJS.ReadableStream, writeStream);
      console.log(`Downloaded: ${key} -> ${localPath}`);
    }
  } catch (error) {
    console.error(`Error downloading ${key}:`, error);
    throw error;
  }
}

/**
 * List all objects in an S3 folder
 */
async function listS3Objects(folderPrefix: string): Promise<string[]> {
  const objects: string[] = [];
  let continuationToken: string | undefined;

  console.log(`Searching for objects with prefix: "${folderPrefix}"`);

  do {
    const command = new ListObjectsV2Command({
      Bucket: S3_BUCKET_NAME,
      Prefix: folderPrefix,
      ContinuationToken: continuationToken,
    });

    const response = await s3Client.send(command);

    console.log(
      `API Response: Found ${
        response.Contents?.length || 0
      } objects in this batch`
    );

    if (response.Contents) {
      for (const object of response.Contents) {
        console.log(`Found object: ${object.Key}`);
        if (object.Key && !object.Key.endsWith("/")) {
          // Skip folders (keys ending with '/')
          objects.push(object.Key);
        } else if (object.Key?.endsWith("/")) {
          console.log(`Skipping folder: ${object.Key}`);
        }
      }
    }

    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  return objects;
}

/**
 * List objects in the root of the bucket to see what's available
 */
async function debugListBucketContents(): Promise<void> {
  console.log("\n=== DEBUGGING: Listing bucket contents ===");

  try {
    const command = new ListObjectsV2Command({
      Bucket: S3_BUCKET_NAME,
      MaxKeys: 20, // Limit to first 20 objects for debugging
    });

    const response = await s3Client.send(command);

    if (response.Contents && response.Contents.length > 0) {
      console.log("Found objects in bucket:");
      response.Contents.forEach((object, index) => {
        console.log(`${index + 1}. ${object.Key} (${object.Size} bytes)`);
      });
    } else {
      console.log("No objects found in bucket root");
    }

    console.log("=== END DEBUGGING ===\n");
  } catch (error) {
    console.error("Error listing bucket contents:", error);
  }
}

/**
 * Download an entire folder from S3
 */
async function downloadS3Folder(
  s3FolderPath: string,
  localFolderPath: string
): Promise<void> {
  console.log(`Starting download from S3 folder: ${s3FolderPath}`);
  console.log(`Local destination: ${localFolderPath}`);

  try {
    // List all objects in the folder
    const objects = await listS3Objects(s3FolderPath);
    console.log(`Found ${objects.length} files to download`);

    // Download each file
    for (const objectKey of objects) {
      // Create local path by removing the S3 folder prefix and adding local folder path
      let relativePath = objectKey.replace(s3FolderPath, "");

      // Add extension if the file doesn't have one
      if (!relativePath.includes(".")) {
        relativePath += DEFAULT_AUDIO_EXTENSION;
        console.log(`Adding extension: ${relativePath}`);
      }

      const localPath = join(localFolderPath, relativePath);

      await downloadFile(objectKey, localPath);
    }

    console.log(
      `Successfully downloaded ${objects.length} files from ${s3FolderPath}`
    );
  } catch (error) {
    console.error("Error downloading S3 folder:", error);
    throw error;
  }
}

/**
 * Main function to download the room folders
 */
async function main(): Promise<void> {
  try {
    await debugListBucketContents();

    console.log(`\nStarting download of ${neonDemos.length} rooms...`);
    console.log(`Bucket: ${S3_BUCKET_NAME}`);
    console.log(`Region: ${AWS_REGION}`);

    let successCount = 0;
    let failCount = 0;

    for (const roomName of neonDemos) {
      console.log(`\n--- Processing room: ${roomName} ---`);

      try {
        const s3FolderPath = `${FOLDER_PREFIX}/${roomName}/`;
        const localFolderPath = join(process.cwd(), OUTPUT_DIR, roomName);

        console.log(`Looking for folder: ${s3FolderPath}`);

        await downloadS3Folder(s3FolderPath, localFolderPath);
        successCount++;
        console.log(`Successfully downloaded ${roomName}`);
      } catch (error) {
        failCount++;
        console.error(`Failed to download ${roomName}:`, error);
        // Continue with the next room instead of stopping
      }
    }

    console.log(`\n=== DOWNLOAD SUMMARY ===`);
    console.log(`Total rooms processed: ${neonDemos.length}`);
    console.log(`Successful downloads: ${successCount}`);
    console.log(`Failed downloads: ${failCount}`);
    console.log("Overall download process completed!");
  } catch (error) {
    console.error("Error in main function:", error);
    process.exit(1);
  }
}

// Runner
main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
