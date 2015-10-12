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

package org.rakam.presto.stream;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/01/15 10:54.
 */
public enum StreamErrorCode implements ErrorCodeSupplier
{
    STREAM_ERROR(0x0300_0000);

    private final ErrorCode errorCode;

    StreamErrorCode(int code)
    {
        errorCode = new ErrorCode(code, name());
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
