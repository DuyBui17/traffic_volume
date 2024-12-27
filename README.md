StringBuilder sb = new StringBuilder();
for (int i = 0; i < fields.length; i++) {
    sb.append(fields[i]);
    if (i < fields.length - 1) {
        sb.append(",");
    }
}
result.set(sb.toString());
