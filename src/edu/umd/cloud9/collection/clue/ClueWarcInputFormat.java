/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

/**
 * Hadoop FileInputFormat for reading WARC files
 *
 * (C) 2009 - Carnegie Mellon University
 *
 * 1. Redistributions of this source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. The names "Lemur", "Indri", "University of Massachusetts",
 *    "Carnegie Mellon", and "lemurproject" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. To obtain permission, contact
 *    license@lemurproject.org.
 *
 * 4. Products derived from this software may not be called "Lemur" or "Indri"
 *    nor may "Lemur" or "Indri" appear in their names without prior written
 *    permission of The Lemur Project. To obtain permission,
 *    contact license@lemurproject.org.
 *
 * THIS SOFTWARE IS PROVIDED BY THE LEMUR PROJECT AS PART OF THE CLUEWEB09
 * PROJECT AND OTHER CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN
 * NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * @author mhoy@cs.cmu.edu (Mark J. Hoy)
 */

package edu.umd.cloud9.collection.clue;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.log4j.Logger;


public class ClueWarcInputFormat extends FileInputFormat<LongWritable, ClueWarcRecord> {

	private static final Logger logger = Logger.getLogger(ClueWarcInputFormat.class);

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	/**
	 * Just return the record reader
	 */
	public RecordReader<LongWritable, ClueWarcRecord> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new ClueWarcRecordReader();
	}

	public static class ClueWarcRecordReader extends RecordReader<LongWritable, ClueWarcRecord> {

		private long recordCount = 1;
		private Path path = null;
		private DataInputStream input = null;

		private LongWritable key = new LongWritable();
		private ClueWarcRecord value = new ClueWarcRecord();

		// AAA
		private FileSplit fileSplit;
		private Configuration conf;

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {

			this.fileSplit = (FileSplit) split;
			this.conf = context.getConfiguration();
			this.path = fileSplit.getPath();

			logger.info("Creating input split for path: " + path);

			FileSystem fs = FileSystem.get(conf);
			CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
			CompressionCodec compressionCodec = compressionCodecs.getCodec(path);
			this.input = new DataInputStream(compressionCodec.createInputStream(fs.open(path)));
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			ClueWarcRecord newRecord = ClueWarcRecord.readNextWarcRecord(input);
			if (newRecord == null) {
				return false;
			}	

			newRecord.setWarcFilePath(path.toString());

			value.set(newRecord);
			key.set(recordCount++);			
			return true;
		}

		@Override
		public void close() throws IOException {
			input.close();
		}

		@Override
		public float getProgress() throws IOException {
			return (float) recordCount / 40000f;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
		InterruptedException {
			return key;
		}

		@Override
		public ClueWarcRecord getCurrentValue() throws IOException,
		InterruptedException {
			return value;
		}
	}
}