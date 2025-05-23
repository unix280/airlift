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
package com.facebook.airlift.units;

import jakarta.validation.ConstraintDeclarationException;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public class MinDataSizeValidator
        implements ConstraintValidator<MinDataSize, DataSize>
{
    private DataSize min;

    @Override
    public void initialize(MinDataSize dataSize)
    {
        try {
            this.min = DataSize.valueOf(dataSize.value());
        } catch (IllegalArgumentException e) {
            throw new ConstraintDeclarationException(e);
        }
    }

    @Override
    public boolean isValid(DataSize dataSize, ConstraintValidatorContext context)
    {
        return (dataSize == null) || (dataSize.compareTo(min) >= 0);
    }

    @Override
    public String toString()
    {
        return "min:" + min;
    }
}
