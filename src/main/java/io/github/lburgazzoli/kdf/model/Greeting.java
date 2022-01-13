package io.github.lburgazzoli.kdf.model;

public class Greeting {
    private String message;
    private String time;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "Greeting{" +
            "message='" + message + '\'' +
            ", time='" + time + '\'' +
            '}';
    }
}
