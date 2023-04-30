package com.superior.datatunnel.plugin.elasticsearch.enums;

public enum FieldType {
    Auto("auto"), //
    Text("text"), //
    Keyword("keyword"), //
    Long("long"), //
    Integer("integer"), //
    Short("short"), //
    Byte("byte"), //
    Double("double"), //
    Float("float"), //
    Half_Float("half_float"), //
    Scaled_Float("scaled_float"), //
    Date("date"), //
    Date_Nanos("date_nanos"), //
    Boolean("boolean"), //
    Binary("binary"), //
    Integer_Range("integer_range"), //
    Float_Range("float_range"), //
    Long_Range("long_range"), //
    Double_Range("double_range"), //
    Date_Range("date_range"), //
    Ip_Range("ip_range"), //
    Object("object"), //
    Nested("nested"), //
    Ip("ip"), //
    TokenCount("token_count"), //
    Percolator("percolator"), //
    Flattened("flattened"), //
    Search_As_You_Type("search_as_you_type"), //
    /** @since 4.1 */
    Rank_Feature("rank_feature"), //
    /** @since 4.1 */
    Rank_Features("rank_features"), //
    /** since 4.2 */
    Wildcard("wildcard"), //
    /** @since 4.2 */
    Dense_Vector("dense_vector") //
    ;

    private final String mappedName;

    FieldType(String mappedName) {
        this.mappedName = mappedName;
    }

    public String getMappedName() {
        return mappedName;
    }
}
