package com.superior.datatunnel.plugin.elasticsearch;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.superior.datatunnel.plugin.elasticsearch.enums.*;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A class to hold the mapping parameters that might be set on
 *
 * @author Peter-Josef Meisch
 * @author Aleksei Arsenev
 * @author Brian Kimmig
 * @author Morgan Lutz
 * @author Sascha Woo
 * @since 4.0
 */
public final class MappingParameters {

	static final String FIELD_PARAM_COERCE = "coerce";

	static final String FIELD_PARAM_COPY_TO = "copy_to";

	static final String FIELD_PARAM_DATA = "fielddata";

	static final String FIELD_PARAM_DOC_VALUES = "doc_values";

	static final String FIELD_PARAM_EAGER_GLOBAL_ORDINALS = "eager_global_ordinals";

	static final String FIELD_PARAM_ENABLED = "enabled";

	static final String FIELD_PARAM_FORMAT = "format";

	static final String FIELD_PARAM_IGNORE_ABOVE = "ignore_above";

	static final String FIELD_PARAM_IGNORE_MALFORMED = "ignore_malformed";

	static final String FIELD_PARAM_INDEX = "index";

	static final String FIELD_PARAM_INDEX_OPTIONS = "index_options";

	static final String FIELD_PARAM_INDEX_PHRASES = "index_phrases";

	static final String FIELD_PARAM_INDEX_PREFIXES = "index_prefixes";

	static final String FIELD_PARAM_INDEX_PREFIXES_MIN_CHARS = "min_chars";

	static final String FIELD_PARAM_INDEX_PREFIXES_MAX_CHARS = "max_chars";

	static final String FIELD_PARAM_INDEX_ANALYZER = "analyzer";

	static final String FIELD_PARAM_MAX_SHINGLE_SIZE = "max_shingle_size";

	static final String FIELD_PARAM_NORMALIZER = "normalizer";

	static final String FIELD_PARAM_NORMS = "norms";

	static final String FIELD_PARAM_NULL_VALUE = "null_value";

	static final String FIELD_PARAM_POSITION_INCREMENT_GAP = "position_increment_gap";

	static final String FIELD_PARAM_POSITIVE_SCORE_IMPACT = "positive_score_impact";

	static final String FIELD_PARAM_DIMS = "dims";

	static final String FIELD_PARAM_SCALING_FACTOR = "scaling_factor";

	static final String FIELD_PARAM_SEARCH_ANALYZER = "search_analyzer";

	static final String FIELD_PARAM_STORE = "store";

	static final String FIELD_PARAM_SIMILARITY = "similarity";

	static final String FIELD_PARAM_TERM_VECTOR = "term_vector";

	static final String FIELD_PARAM_TYPE = "type";

	private final String analyzer;

	private final boolean coerce;

	@Nullable
	private final String[] copyTo;

	private final DateFormat[] dateFormats;

	private final String[] dateFormatPatterns;

	private final boolean docValues;

	private final boolean eagerGlobalOrdinals;

	private final boolean enabled;

	private final boolean fielddata;

	@Nullable
	private final Integer ignoreAbove;

	private final boolean ignoreMalformed;

	private final boolean index;

	private final IndexOptions indexOptions;

	private final boolean indexPhrases;

	@Nullable
	private final IndexPrefixes indexPrefixes;

	private final String normalizer;

	private final boolean norms;

	@Nullable
	private final Integer maxShingleSize;

	private final String nullValue;

	private final NullValueType nullValueType;

	private final Integer positionIncrementGap;

	private final boolean positiveScoreImpact;

	private final Integer dims;

	private final String searchAnalyzer;

	private final double scalingFactor;

	private final Similarity similarity;

	private final boolean store;

	private final TermVector termVector;

	private final FieldType type;

	public MappingParameters(FieldMeta field) {
		index = field.isIndex();
		store = field.isStore();
		fielddata = field.isFielddata();
		type = field.getType();
		dateFormats = field.getFormat();
		dateFormatPatterns = field.getPattern();
		analyzer = field.getAnalyzer();
		searchAnalyzer = field.getSearchAnalyzer();
		normalizer = field.getNormalizer();
		copyTo = field.getCopyTo();
		ignoreAbove = field.getIgnoreAbove() >= 0 ? field.getIgnoreAbove() : null;
		coerce = field.isCoerce();
		docValues = field.isDocValues();
		Assert.isTrue(!((type == FieldType.Text || type == FieldType.Nested) && !docValues),
				"docValues false is not allowed for field type text");
		ignoreMalformed = field.isIgnoreMalformed();
		indexOptions = field.getIndexOptions();
		indexPhrases = field.isIndexPhrases();
		indexPrefixes = field.getIndexPrefixes().length > 0 ? field.getIndexPrefixes()[0] : null;
		norms = field.isNorms();
		nullValue = field.getNullValue();
		nullValueType = field.getNullValueType();
		positionIncrementGap = field.getPositionIncrementGap();
		similarity = field.getSimilarity();
		termVector = field.getTermVector();
		scalingFactor = field.getScalingFactor();
		maxShingleSize = field.getMaxShingleSize() >= 0 ? field.getMaxShingleSize() : null;
		Assert.isTrue(type != FieldType.Search_As_You_Type //
				|| maxShingleSize == null //
				|| (maxShingleSize >= 2 && maxShingleSize <= 4), //
				"maxShingleSize must be in inclusive range from 2 to 4 for field type search_as_you_type");
		positiveScoreImpact = field.isPositiveScoreImpact();
		dims = field.getDims();
		if (type == FieldType.Dense_Vector) {
			Assert.isTrue(dims >= 1 && dims <= 2048,
					"Invalid required parameter! Dense_Vector value \"dims\" must be between 1 and 2048.");
		}
		Assert.isTrue(field.isEnabled() || type == FieldType.Object, "enabled false is only allowed for field type object");
		enabled = field.isEnabled();
		eagerGlobalOrdinals = field.isEagerGlobalOrdinals();
	}

	public boolean isStore() {
		return store;
	}

	/**
	 * writes the different fields to an {@link ObjectNode}.
	 *
	 * @param objectNode must not be {@literal null}
	 */
	public void writeTypeAndParametersTo(ObjectNode objectNode) {

		Assert.notNull(objectNode, "objectNode must not be null");

		if (fielddata) {
			objectNode.put(FIELD_PARAM_DATA, fielddata);
		}

		if (type != FieldType.Auto) {
			objectNode.put(FIELD_PARAM_TYPE, type.getMappedName());

			if (type == FieldType.Date || type == FieldType.Date_Nanos || type == FieldType.Date_Range) {
				List<String> formats = new ArrayList<>();

				// built-in formats
				for (DateFormat dateFormat : dateFormats) {
					formats.add(dateFormat.toString());
				}

				// custom date formats
				Collections.addAll(formats, dateFormatPatterns);

				if (!formats.isEmpty()) {
					objectNode.put(FIELD_PARAM_FORMAT, String.join("||", formats));
				}
			}
		}

		if (!index) {
			objectNode.put(FIELD_PARAM_INDEX, index);
		}

		if (StringUtils.hasLength(analyzer)) {
			objectNode.put(FIELD_PARAM_INDEX_ANALYZER, analyzer);
		}

		if (StringUtils.hasLength(searchAnalyzer)) {
			objectNode.put(FIELD_PARAM_SEARCH_ANALYZER, searchAnalyzer);
		}

		if (StringUtils.hasLength(normalizer)) {
			objectNode.put(FIELD_PARAM_NORMALIZER, normalizer);
		}

		if (copyTo != null && copyTo.length > 0) {
			objectNode.putArray(FIELD_PARAM_COPY_TO)
					.addAll(Arrays.stream(copyTo).map(TextNode::valueOf).collect(Collectors.toList()));
		}

		if (ignoreAbove != null) {
			Assert.isTrue(ignoreAbove >= 0, "ignore_above must be a positive value");
			objectNode.put(FIELD_PARAM_IGNORE_ABOVE, ignoreAbove);
		}

		if (!coerce) {
			objectNode.put(FIELD_PARAM_COERCE, coerce);
		}

		if (!docValues) {
			objectNode.put(FIELD_PARAM_DOC_VALUES, docValues);
		}

		if (ignoreMalformed) {
			objectNode.put(FIELD_PARAM_IGNORE_MALFORMED, ignoreMalformed);
		}

		if (indexOptions != IndexOptions.none) {
			objectNode.put(FIELD_PARAM_INDEX_OPTIONS, indexOptions.toString());
		}

		if (indexPhrases) {
			objectNode.put(FIELD_PARAM_INDEX_PHRASES, indexPhrases);
		}

		if (indexPrefixes != null) {
			ObjectNode prefixNode = objectNode.putObject(FIELD_PARAM_INDEX_PREFIXES);
			if (indexPrefixes.minChars() != IndexPrefixes.MIN_DEFAULT) {
				prefixNode.put(FIELD_PARAM_INDEX_PREFIXES_MIN_CHARS, indexPrefixes.minChars());
			}
			if (indexPrefixes.maxChars() != IndexPrefixes.MAX_DEFAULT) {
				prefixNode.put(FIELD_PARAM_INDEX_PREFIXES_MAX_CHARS, indexPrefixes.maxChars());
			}
		}

		if (!norms) {
			objectNode.put(FIELD_PARAM_NORMS, norms);
		}

		if (StringUtils.hasLength(nullValue)) {
			switch (nullValueType) {
				case Integer:
					objectNode.put(FIELD_PARAM_NULL_VALUE, Integer.valueOf(nullValue));
					break;
				case Long:
					objectNode.put(FIELD_PARAM_NULL_VALUE, Long.valueOf(nullValue));
					break;
				case Double:
					objectNode.put(FIELD_PARAM_NULL_VALUE, Double.valueOf(nullValue));
					break;
				case String:
				default:
					objectNode.put(FIELD_PARAM_NULL_VALUE, nullValue);
					break;
			}
		}

		if (positionIncrementGap != null && positionIncrementGap >= 0) {
			objectNode.put(FIELD_PARAM_POSITION_INCREMENT_GAP, positionIncrementGap);
		}

		if (similarity != Similarity.Default) {
			objectNode.put(FIELD_PARAM_SIMILARITY, similarity.toString());
		}

		if (termVector != TermVector.none) {
			objectNode.put(FIELD_PARAM_TERM_VECTOR, termVector.toString());
		}

		if (type == FieldType.Scaled_Float) {
			objectNode.put(FIELD_PARAM_SCALING_FACTOR, scalingFactor);
		}

		if (maxShingleSize != null) {
			objectNode.put(FIELD_PARAM_MAX_SHINGLE_SIZE, maxShingleSize);
		}

		if (!positiveScoreImpact) {
			objectNode.put(FIELD_PARAM_POSITIVE_SCORE_IMPACT, positiveScoreImpact);
		}

		if (type == FieldType.Dense_Vector) {
			objectNode.put(FIELD_PARAM_DIMS, dims);
		}

		if (!enabled) {
			objectNode.put(FIELD_PARAM_ENABLED, enabled);
		}

		if (eagerGlobalOrdinals) {
			objectNode.put(FIELD_PARAM_EAGER_GLOBAL_ORDINALS, eagerGlobalOrdinals);
		}
	}
}
