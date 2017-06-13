import java.util.ListResourceBundle;

/**
 * Created by Денис on 12.06.2017.
 */
public class LocalizedResources_hu_HU extends ListResourceBundle {
    @Override
    protected Object[][] getContents() {
        return new Object[][]{
                {"removeButton", "Töröl"},
                {"addButton", "Hozzáad"},
                {"removeLowerButton", "Távolítsa el az alsó"},
                {"importButton", "Import"},
                {"saveButton", "Megtartása"},
                {"refreshButton", "Frissítés"},
                {"windowTitle", "Laba №8"},
                {"error1", "„Hely” mező nem lehet üres. A helyszín csak a következőket tartalmazhatja cirill, az ábécé, számok, „-” és „_”"},
                {"error2", "„Név” mező nem lehet pustym.V neve tartalmazhat csak cirill és latin ábécé"},
                {"error3", "Életkor csak a 0-tól 120 éves"},
                {"filterButton","Szűrő"},
                {"loc","HU"},
                {"oldDataError","Kísérelte meg a kérelem irreleváns információ, azt frissítették, próbálja megismételni a kérést"},
                {"timedOutError","Nem nyújt be az élet jeleit két percen át, ez történik, hogy újra a szerverhez"},
                {"Column1","Név"},
                {"Column2","Kor"},
                {"Column3","Elhelyezkedés"},
                {"Column4","Létrehozásának időpontja"},
                {"timeZone","UTC+2"},
                {"CannotReachServer","A szerver nem elérhető, vagy az összes port foglalt"},
                {"TtC","Csatlakozás a port: "},
                {"Saved","A gyűjtemény sikeresen egy fájlba"},
                {"FnFError","A fájl nem található"},
                {"Port","Kikötő "},
                {"Unavailable"," nem elérhető"},
                {"Hello","Helló"}
        };
    }
}