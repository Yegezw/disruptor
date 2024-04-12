package com.zzw.collection;

/**
 * 订单事件
 */
public class OrderEvent {

    private int price;
    private String message;

    public void setPrice(int price) {
        this.price = price;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "price=" + price +
                ", message='" + message + '\'' +
                '}';
    }
}
