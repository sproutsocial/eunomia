/*
 * The MIT License (MIT)
 * Copyright (c) 2018 SproutSocial, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */

package groovy.com.sproutsocial.elasticsearch.plugins.eunomia

import com.sproutsocial.elasticsearch.plugins.eunomia.TieredPrioritizedQueueKt
import spock.lang.Specification

class TieredPrioritizedQueue extends Specification {

    def "test extraction extension"() {
        when:
        def result = TieredPrioritizedQueueKt.extractIf(iterable, filter)

        then:
            assert iterable == after
            assert result == extracted

        where:
        iterable          | filter       | after       | extracted
        ["a", "b", "c"]   | {it == "a"}  | ["b", "c"]  | ["a"]
        ["a", "b", "c"]   | {it == "b"}  | ["a", "c"]  | ["b"]
        ["a", "b", "c"]   | {it == "c"}  | ["a", "b"]  | ["c"]
    }
}