/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.udfs;

import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.drill.exec.expr.fn.impl.StringFunctionUtil;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.ops.ContextInformation;

import com.carrotsearch.hppc.mutables.CharHolder;
@SuppressWarnings("unused")
public class Masker {
  
  @FunctionTemplate(name = "mask", scope =  FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Mask implements DrillSimpleFunc {
    @Param VarCharHolder inputStr;
    @Workspace Charset charset;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;
    
    public void setup() {
      charset = java.nio.charset.Charset.forName("UTF-8");
    }

    public void eval() {
      int charCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(inputStr.buffer, inputStr.start, inputStr.end);
      String val = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputStr);
      String[] sub_sections = val.split("-");
      int num_sections = sub_sections.length;
      StringBuilder builder = new StringBuilder();
      for(int i = 0; i < num_sections; i++) {
        if(i == 0) {
          builder.append(sub_sections[i]);
        }
        else {
          builder.append("-");
          String new_val = org.apache.commons.lang3.StringUtils.repeat("1", sub_sections[i].length());
          builder.append(new_val);
        }
      }
      String converted_value = builder.toString();
      buffer.setBytes(0, converted_value.getBytes(charset));
      out.buffer = buffer;
      out.start = 0;
      out.end = converted_value.length();
    }
  }
  
  @FunctionTemplate(name = "mask_nullable", scope =  FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class MaskNullable implements DrillSimpleFunc {
    @Param NullableVarCharHolder inputStr;
    @Workspace Charset charset;
    @Output NullableVarCharHolder out;
    @Inject DrillBuf buffer;
    
    public void setup() {
      charset = java.nio.charset.Charset.forName("UTF-8");
    }

    public void eval() {
      int charCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(inputStr.buffer, inputStr.start, inputStr.end);
      String val = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputStr.start,  inputStr.end,  inputStr.buffer);
      String[] sub_sections = val.split("-");
      int num_sections = sub_sections.length;
      StringBuilder builder = new StringBuilder();
      for(int i = 0; i < num_sections; i++) {
        if(i == 0) {
          builder.append(sub_sections[i]);
        }
        else {
          builder.append("-");
          String new_val = org.apache.commons.lang3.StringUtils.repeat("1", sub_sections[i].length());
          builder.append(new_val);
        }
      }
      String converted_value = builder.toString();
      buffer.setBytes(0, converted_value.getBytes(charset));
      out.buffer = buffer;
      out.start = 0;
      out.end = converted_value.length();
      out.isSet = 1;
    }
  }

@FunctionTemplate(name = "mask", scope =  FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public static class MaskWPattern implements DrillSimpleFunc {
  @Param VarCharHolder inputStr;
  @Param(constant=true) VarCharHolder inputPattern;
  @Param(constant=true) VarCharHolder separator;
  @Param(constant=true) VarCharHolder replaceChar;
  @Workspace Charset charset;
  @Output VarCharHolder out;
  @Inject DrillBuf buffer;
  
  public void setup() {
    charset = java.nio.charset.Charset.forName("UTF-8");
  }

  public void eval() {
    int charCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(inputStr.buffer, inputStr.start, inputStr.end);
    String val = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputStr);
    char sep = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(separator).charAt(0);
    char replace_char = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(replaceChar).charAt(0);
    String[] sub_sections = val.split(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPattern));
    int num_sections = sub_sections.length;
    StringBuilder builder = new StringBuilder();
    for(int i = 0; i < num_sections; i++) {
      if(i == 0) {
        // For the special case where the char separator is found at the end of the string
        // but no characters to mask after the separator sign.
        if(num_sections == 0 && sub_sections[i].length() < charCount) {
          builder.append(sub_sections[i]);
          builder.append(sep);
        } else {
          builder.append(sub_sections[i]);
        }
      }
      else {
        builder.append(sep);
        String new_val = org.apache.commons.lang3.StringUtils.repeat(replace_char, sub_sections[i].length());
        builder.append(new_val);
      }
    }
    String converted_value = builder.toString();
    buffer.setBytes(0, converted_value.getBytes(charset));
    out.buffer = buffer;
    out.start = 0;
    out.end = converted_value.length();
  }
}

@FunctionTemplate(name = "mask_nullable", scope =  FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public static class MaskNullableWPattern implements DrillSimpleFunc {
  @Param NullableVarCharHolder inputStr;
  @Param(constant=true) VarCharHolder inputPattern;
  @Param(constant=true) VarCharHolder separator;
  @Param(constant=true) VarCharHolder replaceChar;
  @Workspace Charset charset;
  @Output NullableVarCharHolder out;
  @Inject DrillBuf buffer;
  
  public void setup() {
    charset = java.nio.charset.Charset.forName("UTF-8");
  }

  public void eval() {
    int charCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(inputStr.buffer, inputStr.start, inputStr.end);
    String val = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputStr.start,  inputStr.end,  inputStr.buffer);
    char sep = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(separator).charAt(0);
    char replace_char = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(replaceChar).charAt(0);
    String[] sub_sections = val.split(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(inputPattern));
    int num_sections = sub_sections.length;
    StringBuilder builder = new StringBuilder();
    for(int i = 0; i < num_sections; i++) {
      if(i == 0) {
        // For the special case where the char separator is found at the end of the string
        // but no characters to mask after the separator sign.
        if(num_sections == 0 && sub_sections[i].length() < charCount) {
          builder.append(sub_sections[i]);
          builder.append(sep);
        } else {
          builder.append(sub_sections[i]);
        }
      }
      else {
        builder.append(sep);
        String new_val = org.apache.commons.lang3.StringUtils.repeat(replace_char, sub_sections[i].length());
        builder.append(new_val);
      }
    }
    String converted_value = builder.toString();
    buffer.setBytes(0, converted_value.getBytes(charset));
    out.buffer = buffer;
    out.start = 0;
    out.end = converted_value.length();
    out.isSet = 1;
  }
}

}

