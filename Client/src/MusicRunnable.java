/**
 * Created by Алексей on 06.06.2017.
 */
public class MusicRunnable implements Runnable{
    String path;
    @Override
    public void run() {
            new Music(path);
        }
}
