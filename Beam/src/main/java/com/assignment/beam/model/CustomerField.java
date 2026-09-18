package com.assignment.beam.model;

/**
 * CustomerField is a helper class associated to CustomerRecord to define the attributes and their properties.
 */
public enum CustomerField {

    CUSTOMER_ID("customer_id", "customer_id", 7, 7, "^C\\d{6}$", true, false, false),
    FIRST_NAME("first_name", "first_name", 3, 15, "[A-Za-z][A-Za-z .'-]*", true, false, false),
    LAST_NAME("last_name", "last_name", 0, 15, "[A-Za-z][A-Za-z .'-]*", true, false, false),
    EMAIL("email", "email", 13, 30, "[^@\\s]+@[^@\\s.]+(\\.[^@\\s.]+)+", true, false, false),
    PHONE_NUMBER("phone_number", "phone_number", 10, 15, "^(\\+\\d{2}-)?\\d{5}-?\\d{5}$", false, true, false),
    SIGNUP_DATE("signup_date", "signup_date", 10, 10, "^\\d{4}-\\d{2}-\\d{2}$", true, false, false),
    SEGMENT("segment", "segment", 3, 20, "[A-Za-z][A-Za-z ]*", true, false, false),
    OCCUPATION("occupation", "occupation", 3, 30, "[A-Za-z][A-Za-z ]*", false, false, false),
    ANNUAL_SPEND("annual_spend", "annual_spend", 1, 15, "^\\d{1,12}(\\.\\d{1,2})?$", true, true, false),
    CURRENCY("currency", "currency", 3, 3, "^[A-Z]{3}$", false, false, false),
    ACCOUNT_STATUS("account_status", "account_status", 3, 13, "[A-Za-z][A-Za-z-]*", false, false, false),
    REFERRED_BY("referred_by", "referred_by", 7, 7, "^C\\d{6}$", false, false, false),
    IS_ACTIVE("is_active", "is_active", 4, 5, "^(true|false)$", false, false, false),
    SKILLS("skills", "skills", 2, 100, "[A-Za-z][A-Za-z .+#-]{1,29}", true, false, true),
    ADDRESS_STREET("address.street", "address_street", 3, 60, null, true, false, false),
    ADDRESS_CITY("address.city", "address_city", 2, 30, "[A-Za-z][A-Za-z .'-]*", true, false, false),
    ADDRESS_STATE("address.state", "address_state", 2, 20, "[A-Za-z][A-Za-z .'-]*", true, false, false),
    ADDRESS_POSTAL_CODE("address.postal_code", "address_postal_code", 6, 6, "^\\d{6}$", true, false, false),
    ADDRESS_COUNTRY("address.country", "address_country", 2, 20, "[A-Za-z][A-Za-z .'-]*", true, false, false),
    EMERGENCY_NAME("emergency_contact.name", "emergency_name", 3, 30, "[A-Za-z][A-Za-z .'-]*", true, false, false),
    EMERGENCY_RELATIONSHIP("emergency_contact.relationship", "emergency_relationship", 3, 20, "[A-Za-z][A-Za-z ]*", true, false, false),
    EMERGENCY_PHONE("emergency_contact.phone", "emergency_phone", 10, 15, "^(\\+\\d{2}-)?\\d{5}-?\\d{5}$", true, true, false),
    EMERGENCY_EMAIL("emergency_contact.email", "emergency_email", 13, 30, "[^@\\s]+@[^@\\s.]+(\\.[^@\\s.]+)+", true, false, false);

    private final String jsonPath;
    private final String dbColumn;
    private final int minLength;
    private final int maxLength;
    private final String pattern;
    private final boolean required;
    private final boolean sensitive;
    private final boolean list;

    CustomerField(String jsonPath, String dbColumn, int minLength, int maxLength,
                  String pattern, boolean required, boolean sensitive, boolean list) {
        this.jsonPath = jsonPath;
        this.dbColumn = dbColumn;
        this.minLength = minLength;
        this.maxLength = maxLength;
        this.pattern = pattern;
        this.required = required;
        this.sensitive = sensitive;
        this.list = list;
    }

    public String jsonPath() {
        return jsonPath;
    }

    public String jsonTopKey() {
        return jsonPath.substring(0, jsonPath.indexOf('.') < 0 ? jsonPath.length() : jsonPath.indexOf('.'));
    }

    public String jsonLeafKey() {
        int i = jsonPath.indexOf('.');
        return i < 0 ? jsonPath : jsonPath.substring(i + 1);
    }

    public String dbColumn() {
        return dbColumn;
    }

    public int minLength() {
        return minLength;
    }

    public int maxLength() {
        return maxLength;
    }

    public String pattern() {
        return pattern;
    }

    public boolean required() {
        return required;
    }

    public boolean sensitive() {
        return sensitive;
    }

    public boolean list() {
        return list;
    }
}
