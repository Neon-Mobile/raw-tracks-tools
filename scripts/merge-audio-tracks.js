#!/usr/bin/env node

import * as fs from 'node:fs';
import * as path from 'node:path';
import { execSync } from 'node:child_process';

const CACHE_DIR = 'liam-recordings';
const OUTPUT_DIR = 'liam-merge-recordings';
const CODEC = 'wav'; // 'aac' or 'wav'

const SELECTIVE_PROCESSING = {
  // Set to false to process all rooms in the [CACHE_DIR]
  enabled: true,
  rooms: [
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
    "Neon-Demo-1763675025826"
  ]
};

const PERFORMANCE_CONFIG = {
  batchSize: 10, // M4 Max can handle 8-10 concurrent rooms
  pauseBetweenBatches: 500,
};

// Ensure output directory exists
if (!fs.existsSync(OUTPUT_DIR)) {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

/**
 * Generate a manifest file for a room folder
 */
async function generateManifest(roomPath) {
  const manifestPath = path.join(roomPath, 'manifest.json');

  try {
    console.log(`Generating manifest for ${roomPath}...`);
    execSync(`npm run gen-manifest -- -i "${roomPath}"`, {
      // stdio: 'inherit',
      stdio: 'pipe',
      cwd: process.cwd()
    });

    if (fs.existsSync(manifestPath)) {
      console.log(`Manifest generated: ${manifestPath}`);
      return manifestPath;
    } else {
      console.log(`Manifest not found after generation: ${manifestPath}`);
      return null;
    }
  } catch (error) {
    console.error(`Error generating manifest for ${roomPath}:`, error.message);
    return null;
  }
}

/**
 * Group audio files by their timestamp prefix
 */
function groupFilesByTimestamp(audioFiles) {
  const groups = {};

  for (const file of audioFiles) {
    const filename = path.basename(file);
    // Extract timestamp from filename (first part before the first hyphen)
    const timestampMatch = filename.match(/^(\d+)-/);

    if (timestampMatch) {
      const timestamp = timestampMatch[1];
      if (!groups[timestamp]) {
        groups[timestamp] = [];
      }
      groups[timestamp].push(file);
    } else {
      console.log(`Couldn't extract timestamp from: ${filename}`);
    }
  }

  return groups;
}

/**
 * Normalize a group of audio files with the same timestamp using multiple -i inputs
 */
async function normalizeTimestampGroup(roomName, timestamp, audioFiles) {
  try {
    console.log(`  Normalizing timestamp group ${timestamp} (${audioFiles.length} files)...`);

    const roomOutputDir = path.join(OUTPUT_DIR, roomName);
    if (!fs.existsSync(roomOutputDir)) {
      fs.mkdirSync(roomOutputDir, { recursive: true });
    }

    const finalOutputPath = path.join(roomOutputDir, `${timestamp}_merged.${CODEC}`);

    // Check if already merged
    if (fs.existsSync(finalOutputPath)) {
      console.log(`    ⏭Already merged: ${timestamp}_merged.${CODEC} (skipping)`);
      return finalOutputPath;
    }

    const normalizedFiles = [];

    // Normalize each file individually
    for (const audioFile of audioFiles) {
      const basename = path.basename(audioFile, path.extname(audioFile));
      const normalizeCommand = `npm run normalize-track -- -i "${audioFile}" -o "${roomOutputDir}" --audio-codec ${CODEC}`;

      console.log(`    Normalizing: ${basename}`);

      try {
        execSync(normalizeCommand, {
          // stdio: 'inherit',
          stdio: 'pipe', // Reduce output noise --- IGNORE ---
          cwd: process.cwd()
        });

        // The normalize-track tool creates {basename}_normalized.{CODEC}
        const normalizedFile = path.join(roomOutputDir, `${basename}_normalized.${CODEC}`);
        if (fs.existsSync(normalizedFile)) {
          normalizedFiles.push(normalizedFile);
          console.log(`    Normalized: ${basename}_normalized.${CODEC}`);
        } else {
          console.log(`    Expected normalized file not found: ${basename}_normalized.${CODEC}`);
        }
      } catch (error) {
        console.error(`    Error normalizing ${basename}:`, error.message);
      }
    }

    if (normalizedFiles.length === 0) {
      console.log(`    No files were successfully normalized`);
      return null;
    }

    // Now merge the normalized files using FFmpeg
    if (normalizedFiles.length === 1) {
      // Only one file, copy it instead of renaming to keep the normalized version
      fs.copyFileSync(normalizedFiles[0], finalOutputPath);
      console.log(`    Single file copied to: ${timestamp}_merged.${CODEC}`);
      console.log(`    Normalized file kept: ${path.basename(normalizedFiles[0])}`);
      return finalOutputPath;
    } else {
      const inputs = normalizedFiles.flatMap(file => ['-i', file]);
      const filterComplex = normalizedFiles.map((_, i) => `[${i}:0]`).join('') + `amix=inputs=${normalizedFiles.length}:duration=longest[out]`;

      const codecArgs = CODEC === 'wav'
        ? ['-ar', '48000', '-ac', '1', '-c:a', 'pcm_s16le']
        : ['-ar', '48000', '-c:a', 'aac', '-b:a', '320k'];

      const mergeCommand = [
        'ffmpeg',
        ...inputs,
        '-filter_complex',
        filterComplex,
        '-map', '[out]',
        ...codecArgs,
        '-y',
        finalOutputPath
      ];

      console.log(`    Merging ${normalizedFiles.length} normalized files...`);

      try {
        execSync(mergeCommand.join(' '), {
          stdio: 'inherit',
          // stdio: 'pipe', // Reduce output noise --- IGNORE ---
          cwd: process.cwd()
        });

        console.log(`    Merged ${normalizedFiles.length} files to: ${timestamp}_merged.${CODEC}`);
        return finalOutputPath;
      } catch (error) {
        console.error(`    Error merging files:`, error.message);
        return null;
      }
    }
  } catch (error) {
    console.error(`    Error processing timestamp group ${timestamp}:`, error.message);
    return null;
  }
}

/**
 * Process a single room folder
 */
async function processRoom(roomName) {
  const roomPath = path.join(CACHE_DIR, roomName);

  if (!fs.existsSync(roomPath)) {
    console.log(`Room folder not found: ${roomPath}`);
    return false;
  }

  console.log(`\n--- Processing room: ${roomName} ---`);

  // Get all audio files in the room
  const files = fs.readdirSync(roomPath);
  const audioFiles = files
    .filter(file => file.endsWith('.webm'))
    .map(file => path.join(roomPath, file));

  if (audioFiles.length === 0) {
    console.log(`No audio files found in ${roomPath}`);
    return false;
  }

  console.log(`Found ${audioFiles.length} audio files`);

  // Generate manifest (optional, for reference)
  await generateManifest(roomPath);

  // Group files by timestamp
  const timestampGroups = groupFilesByTimestamp(audioFiles);
  const timestamps = Object.keys(timestampGroups);

  console.log(`Found ${timestamps.length} timestamp groups: ${timestamps.join(', ')}`);

  const mergedFiles = [];

  // Process each timestamp group
  for (const timestamp of timestamps) {
    const groupFiles = timestampGroups[timestamp];
    console.log(`\n  Processing timestamp group ${timestamp} (${groupFiles.length} files):`);

    // Normalize and combine all files in this timestamp group
    const normalizedFile = await normalizeTimestampGroup(roomName, timestamp, groupFiles);

    if (normalizedFile) {
      mergedFiles.push(normalizedFile);
    }
  }

  console.log(`\n${roomName} processing complete. Created ${mergedFiles.length} normalized files.`);
  mergedFiles.forEach(file => console.log(`   - ${path.basename(file)}`));

  return mergedFiles.length > 0;
}

/**
 * Process rooms in batches with limited concurrency
 */
async function processBatch(roomNames, batchNumber, totalBatches) {
  console.log(`\n--- Processing batch ${batchNumber}/${totalBatches} (${roomNames.length} rooms) ---`);

  const results = await Promise.allSettled(
    roomNames.map(async (roomName) => {
      try {
        const success = await processRoom(roomName);
        return { roomName, success, error: null };
      } catch (error) {
        console.error(`Error processing ${roomName}:`, error.message);
        return { roomName, success: false, error: error.message };
      }
    })
  );

  return results.map(result => result.value);
}

/**
 * Main function
 */
async function main() {
  console.log('Audio Track Merger Started');
  console.log(`Looking for rooms in: ${CACHE_DIR}`);
  console.log(`Output directory: ${OUTPUT_DIR}`);

  if (!fs.existsSync(CACHE_DIR)) {
    console.error(`Cache directory not found: ${CACHE_DIR}`);
    process.exit(1);
  }

  const roomFolders = fs.readdirSync(CACHE_DIR).filter(item => {
    const itemPath = path.join(CACHE_DIR, item);
    return fs.statSync(itemPath).isDirectory();
  });

  // Apply selective processing filter if enabled
  let targetRooms = roomFolders;
  if (SELECTIVE_PROCESSING.enabled) {
    if (SELECTIVE_PROCESSING.rooms.length === 0) {
      console.error(`Selective processing is enabled but no rooms specified in SELECTIVE_PROCESSING.rooms`);
      process.exit(1);
    }

    targetRooms = roomFolders.filter(room => SELECTIVE_PROCESSING.rooms.includes(room));

    const missingRooms = SELECTIVE_PROCESSING.rooms.filter(room => !roomFolders.includes(room));
    if (missingRooms.length > 0) {
      console.warn(`Warning: The following rooms were not found: ${missingRooms.join(', ')}`);
    }

    console.log(`Selective processing enabled. Processing ${targetRooms.length} of ${roomFolders.length} available rooms:`);
    console.log(`   Target rooms: ${SELECTIVE_PROCESSING.rooms.join(', ')}`);
    console.log(`   Found rooms: ${targetRooms.join(', ')}`);
  } else {
    console.log(`Processing all ${roomFolders.length} room folders`);
  }

  if (targetRooms.length === 0) {
    console.log(`No rooms to process`);
    process.exit(0);
  }

  const BATCH_SIZE = PERFORMANCE_CONFIG.batchSize;
  const batches = [];

  // Split rooms into batches
  for (let i = 0; i < targetRooms.length; i += BATCH_SIZE) {
    batches.push(targetRooms.slice(i, i + BATCH_SIZE));
  }

  console.log(`Processing in ${batches.length} batches of up to ${BATCH_SIZE} rooms each`);

  let successCount = 0;
  let failCount = 0;

  // Process each batch sequentially, but rooms within each batch in parallel
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    const batchResults = await processBatch(batch, i + 1, batches.length);

    // Collect results
    for (const result of batchResults) {
      if (result.success) {
        successCount++;
      } else {
        failCount++;
      }
    }

    // Add a brief pause between batches to avoid overwhelming the system
    if (i < batches.length - 1) {
      console.log(`Batch ${i + 1} complete. Waiting ${PERFORMANCE_CONFIG.pauseBetweenBatches / 1000} seconds before next batch...`);
      await new Promise(resolve => setTimeout(resolve, PERFORMANCE_CONFIG.pauseBetweenBatches));
    }
  }

  console.log(`\n=== MERGE SUMMARY ===`);
  console.log(`Total rooms processed: ${targetRooms.length}`);
  console.log(`Successful merges: ${successCount}`);
  console.log(`Failed merges: ${failCount}`);
  console.log(`Audio merge process completed!`);
}

// Runner
main().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});