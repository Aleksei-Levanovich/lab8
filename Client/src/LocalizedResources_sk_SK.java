import java.util.ListResourceBundle;

/**
 * Created by Денис on 12.06.2017.
 */
public class LocalizedResources_sk_SK extends ListResourceBundle {
    @Override
    protected Object[][] getContents() {
        return new Object[][]{
                {"removeButton", "Odstrániť"},
                {"addButton", "Pridať"},
                {"removeLowerButton", "Odobrať nižší"},
                {"importButton", "Import"},
                {"saveButton", "Udržať"},
                {"refreshButton", "Аktualizovať"},
                {"windowTitle", "Laba №8"},
                {"error1", "Pole \"Namiesto\" nemôže byť prázdny. Umiestnenie môže obsahovať iba azbuke, abeceda, čísla, \"-\" a \"_\""},
                {"error2", "Pole \"Name\" nemôže byť názov pustym.V môže obsahovať iba Cyrillic a latinskej abecedy"},
                {"error3", "Starobe môže byť iba v rozmedzí od 0 do 120 rokov"},
                {"filterButton","Filter"},
                {"loc","SK"},
                {"oldDataError","Pokúsili ste sa podať žiadosť o nepodstatné informácie, ktoré boli aktualizované, pokúsiť sa zopakovať svoju požiadavku"},
                {"timedOutError","Ste nepredložili známky života pre viac ako dve minúty, to robí znova pripojiť k serveru"},
                {"Column1","Názov"},
                {"Column2","Vek"},
                {"Column3","Umiestnenia"},
                {"Column4","Čas vytvorenia"},
                {"timeZone","UTC+2"},
                {"CannotReachServer","Server nie je k dispozícii, alebo všetky porty sú zaneprázdnení"},
                {"TtC","Pripojenie k portu: "},
                {"Saved","Zbierka bola úspešne uložená do súboru"},
                {"FnFError","Súbor nenájdený"},
                {"Port","Prístav "},
                {"Unavailable"," nedostupné"},
                {"Hello","Ahoj"}
        };
    }
}