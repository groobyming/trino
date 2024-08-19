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

package com.tencent.bkdata;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
// import org.apache.hadoop.fs.FSDataInputStream;
// import org.apache.hadoop.fs.FileSystem;
// import org.apache.hadoop.fs.Path;

// import java.io.IOException;
// import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class BloomFilterIndex
        implements Serializable
{
    static final long serialVersionUID = -1;

    // column -> filter bytes
    private Map<String, BloomFilter<String>> index;

    private long rowLen;

    public BloomFilterIndex(long rowLen)
    {
        index = new HashMap<>();
        this.rowLen = rowLen;
    }

    public BloomFilter<String> getColumnFilter(String columnName)
    {
        if (!index.containsKey(columnName)) {
            BloomFilter<String> filter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), rowLen, 0.01);
            index.put(columnName, filter);
            return filter;
        }
        else {
            return index.get(columnName);
        }
    }

    public void putBloom(String columnName, String data)
    {
        getColumnFilter(columnName).put(data);
    }

    public boolean isEmpty()
    {
        return index.isEmpty();
    }

    // public static BloomFilterIndex readFrom(FileSystem fs, Path indexPath)
    // {
    //     try {
    //         FSDataInputStream inputStream = fs.open(indexPath);
    //         ObjectInputStream objInput = new ObjectInputStream(inputStream);
    //
    //         try {
    //             BloomFilterIndex idx = (BloomFilterIndex) objInput.readObject();
    //             objInput.close();
    //             return idx;
    //         }
    //         catch (ClassNotFoundException e) {
    //         }
    //     }
    //     catch (IOException e) {
    //     }
    //     return null;
    // }
    //
    // public static BloomFilterIndex readFor(FileSystem fs, Path dataPath)
    // {
    //     return readFrom(fs, getIndexPath(dataPath));
    // }
    //
    // public static Path getIndexPath(Path dataPath)
    // {
    //     return new Path(dataPath.toString().replace("/table_", "/_index/table_") + ".index");
    // }

    public boolean notMatches(String columnName, String target)
    {
        if (!index.containsKey(columnName)) {
            return false; // 列没有索引，没有判断依据，返回可能有
        }
        return !index.get(columnName).mightContain(target);
    }
}
