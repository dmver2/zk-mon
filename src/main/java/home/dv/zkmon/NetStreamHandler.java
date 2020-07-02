package home.dv.zkmon;

public interface NetStreamHandler {
    boolean readBytes(Integer ioBytes,
                      NioController.Att actionInfo);
}