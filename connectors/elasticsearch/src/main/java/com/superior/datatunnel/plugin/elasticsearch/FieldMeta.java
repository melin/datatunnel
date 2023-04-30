package com.superior.datatunnel.plugin.elasticsearch;

import com.superior.datatunnel.plugin.elasticsearch.enums.*;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class FieldMeta {

    @NotBlank(message = "name can not blank")
    private String name;

    private FieldType type = FieldType.Auto;

    private boolean index = true;

    private DateFormat[] format = { DateFormat.date_optional_time, DateFormat.epoch_millis };

    private String[] pattern;

    private boolean store = false;

    private boolean fielddata = false;

    private String searchAnalyzer;

    private String analyzer;

    private String normalizer;

    private String[] ignoreFields;

    private boolean includeInParent = false;

    private String[] copyTo;

    private int ignoreAbove = -1;

    private boolean coerce = true;

    private boolean docValues = true;

    private boolean ignoreMalformed = false;

    private IndexOptions indexOptions = IndexOptions.none;

    private boolean indexPhrases = false;

    private IndexPrefixes[] indexPrefixes;

    private boolean norms = true;

    private String nullValue = "";

    private int positionIncrementGap = -1;

    private Similarity similarity = Similarity.Default;

    private TermVector termVector = TermVector.none;

    private double scalingFactor = 1;

    private int maxShingleSize = -1;

    private boolean storeNullValue = false;

    private boolean positiveScoreImpact = true;

    private boolean enabled = true;

    private boolean eagerGlobalOrdinals = false;

    private NullValueType nullValueType = NullValueType.String;

    private int dims = -1;

    private Dynamic dynamic = Dynamic.INHERIT;
}
