package com.superior.datatunnel.plugin.elasticsearch.enums;

/**
 * Values based on reference doc - https://www.elastic.co/guide/reference/mapping/date-format/. The patterns are taken
 * from this documentation and slightly adapted so that a Java {@link java.time.format.DateTimeFormatter} produces the
 * same values as the Elasticsearch formatter.
 *
 * @author Jakub Vavrik
 * @author Tim te Beek
 * @author Peter-Josef Meisch
 * @author Sascha Woo
 */
public enum DateFormat {
	basic_date("uuuuMMdd"), //
	basic_date_time("uuuuMMdd'T'HHmmss.SSSXXX"), //
	basic_date_time_no_millis("uuuuMMdd'T'HHmmssXXX"), //
	basic_ordinal_date("uuuuDDD"), //
	basic_ordinal_date_time("yyyyDDD'T'HHmmss.SSSXXX"), //
	basic_ordinal_date_time_no_millis("yyyyDDD'T'HHmmssXXX"), //
	basic_time("HHmmss.SSSXXX"), //
	basic_time_no_millis("HHmmssXXX"), //
	basic_t_time("'T'HHmmss.SSSXXX"), //
	basic_t_time_no_millis("'T'HHmmssXXX"), //
	basic_week_date("YYYY'W'wwe"), // week-based-year!
	basic_week_date_time("YYYY'W'wwe'T'HHmmss.SSSX"), // here Elasticsearch uses a different zone format
	basic_week_date_time_no_millis("YYYY'W'wwe'T'HHmmssX"), //
	date("uuuu-MM-dd"), //
	date_hour("uuuu-MM-dd'T'HH"), //
	date_hour_minute("uuuu-MM-dd'T'HH:mm"), //
	date_hour_minute_second("uuuu-MM-dd'T'HH:mm:ss"), //
	date_hour_minute_second_fraction("uuuu-MM-dd'T'HH:mm:ss.SSS"), //
	date_hour_minute_second_millis("uuuu-MM-dd'T'HH:mm:ss.SSS"), //
	date_optional_time("uuuu-MM-dd['T'HH:mm:ss.SSSXXX]"), //
	date_time("uuuu-MM-dd'T'HH:mm:ss.SSSXXX"), //
	date_time_no_millis("uuuu-MM-dd'T'HH:mm:ssVV"), // here Elasticsearch uses the zone-id in it's implementation
	epoch_millis("epoch_millis"), //
	epoch_second("epoch_second"), //
	hour("HH"), //
	hour_minute("HH:mm"), //
	hour_minute_second("HH:mm:ss"), //
	hour_minute_second_fraction("HH:mm:ss.SSS"), //
	hour_minute_second_millis("HH:mm:ss.SSS"), //
	ordinal_date("uuuu-DDD"), //
	ordinal_date_time("uuuu-DDD'T'HH:mm:ss.SSSXXX"), //
	ordinal_date_time_no_millis("uuuu-DDD'T'HH:mm:ssXXX"), //
	time("HH:mm:ss.SSSXXX"), //
	time_no_millis("HH:mm:ssXXX"), //
	t_time("'T'HH:mm:ss.SSSXXX"), //
	t_time_no_millis("'T'HH:mm:ssXXX"), //
	week_date("YYYY-'W'ww-e"), //
	week_date_time("YYYY-'W'ww-e'T'HH:mm:ss.SSSXXX"), //
	week_date_time_no_millis("YYYY-'W'ww-e'T'HH:mm:ssXXX"), //
	weekyear(""), // no TemporalAccessor available for these 3
	weekyear_week(""), //
	weekyear_week_day(""), //
	year("uuuu"), //
	year_month("uuuu-MM"), //
	year_month_day("uuuu-MM-dd"); //

	private final String pattern;

	DateFormat(String pattern) {
		this.pattern = pattern;
	}

	/**
	 * @since 4.2
	 */
	public String getPattern() {
		return pattern;
	}
}
