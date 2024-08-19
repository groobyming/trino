/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.plugin.hive.parquet;

import com.tencent.bkdata.BloomFilterIndex;
// import io.trino.filesystem.TrinoInputFile;
// import io.trino.plugin.hive.HdfsEnvironment;
// import io.trino.spi.security.ConnectorIdentity;
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.FileSystem;
// import org.apache.hadoop.fs.Path;

public class Bloom
{
    BloomFilterIndex filter;

    // public Bloom(HdfsEnvironment hdfsEnvironment, Path dataPath, ConnectorIdentity identity, Configuration conf)
    // {
    //     try {
    //         FileSystem fileSystem = hdfsEnvironment
    //                 .getFileSystem(identity, dataPath, conf);
    //         this.filter = BloomFilterIndex.readFor(fileSystem, dataPath);
    //     }
    //     catch (Exception e) {
    //     }
    // }

    public boolean notMatches(String columnName, String target)
    {
        if (filter == null) {
            return false;
        }

        try {
            Boolean result = filter.notMatches(columnName, target);
            return result;
        }
        catch (Exception e) {
            return false;
        }
    }
}
