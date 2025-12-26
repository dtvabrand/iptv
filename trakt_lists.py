import sys, io, requests, re, time, os, json, tempfile
from datetime import datetime
from typing import Any, List
from lxml import html
from urllib.parse import unquote
from collections import namedtuple, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SITE_WORKERS=8
WD_WORKERS=16
HTTP_TIMEOUT=(4,10)

sys.stdout=io.TextIOWrapper(sys.stdout.buffer,encoding='utf-8',line_buffering=True)
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
WFilm=namedtuple('WFilm','title date_iso url page is_full link_state',defaults=(True,''))
TODAY=datetime.now().date(); CUR_YEAR=TODAY.year
S=requests.Session(); S.headers["User-Agent"]=UA
ad=HTTPAdapter(max_retries=Retry(total=2,backoff_factor=0.6,status_forcelist=(429,500,502,503,504),allowed_methods=frozenset({"GET","POST"})),pool_connections=64,pool_maxsize=64)
S.mount("https://",ad); S.mount("http://",ad)
MONTHS=("january","february","march","april","may","june","july","august","september","october","november","december")
DATE_FULL_RE=re.compile(r"\b("+"|".join(m.capitalize() for m in MONTHS)+r")\s+(\d{1,2}),\s*(\d{4})\b",re.IGNORECASE)
YEAR_RE=re.compile(r"\b((?:19|20)\d{2})\b")

SITES=[
    (f"https://en.wikipedia.org/wiki/List_of_American_films_of_{CUR_YEAR}", f"🎟️ American Films of {CUR_YEAR}", "", f"american-films-of-{CUR_YEAR}", [1,2,3,4]),
    ("https://en.wikipedia.org/wiki/List_of_Universal_Pictures_films_(2020%E2%80%932029)","🌎 Universal Pictures","", "universal-pictures",0),
    ("https://en.wikipedia.org/wiki/List_of_Paramount_Pictures_films_(2020%E2%80%932029)","🏔️ Paramount Pictures","", "paramount-pictures",0),
    ("https://en.wikipedia.org/wiki/List_of_Warner_Bros._films_(2020%E2%80%932029)","🛡️ Warner Bros. Pictures","", "warner-bros-pictures",0),
    ("https://en.wikipedia.org/wiki/List_of_Walt_Disney_Studios_films_(2020%E2%80%932029)","💫 Walt Disney Studios","", "walt-disney-studios",0),
    ("https://en.wikipedia.org/wiki/List_of_Columbia_Pictures_films_(2020%E2%80%932029)","🗽 Columbia Pictures","", "columbia-pictures",0),
    ("https://en.wikipedia.org/wiki/List_of_adaptations_of_works_by_Stephen_King","👀 Stephen King","", "stephen-king",[0,1,4]),
    ("https://en.wikipedia.org/wiki/List_of_Walt_Disney_Animation_Studios_films","🪄 Ink & Magic – A Disney Animation Journey","WDAS","ink-magic-a-disney-animation-journey",0),
    ("https://en.wikipedia.org/wiki/List_of_Disney_Television_Animation_productions","🪄 Ink & Magic – A Disney Animation Journey","DTVA","ink-magic-a-disney-animation-journey",[4,5]),
    ("https://en.wikipedia.org/wiki/Disneytoon_Studios","🪄 Ink & Magic – A Disney Animation Journey","DTS","ink-magic-a-disney-animation-journey",0),
    ("https://en.wikipedia.org/wiki/List_of_Disney%2B_original_films","🪄 Ink & Magic – A Disney Animation Journey","D+","ink-magic-a-disney-animation-journey",{"tables":0,"href_contains":"animated"}),
    ("https://en.wikipedia.org/wiki/List_of_Pixar_films","🪄 Ink & Magic – A Disney Animation Journey","Pixar","ink-magic-a-disney-animation-journey",0),
    ("https://en.wikipedia.org/wiki/List_of_films_based_on_Marvel_Comics_publications","✪ Marvel Comics: Live-Action Films","", "marvel-comics-live-action-films",0),
    ("https://en.wikipedia.org/wiki/List_of_Warner_Bros._Pictures_Animation_productions","🐰 Warner Bros. Toons — Films & TV","WBPA","warner-bros-toons-films-tv",0),
    ("https://en.wikipedia.org/wiki/List_of_Warner_Bros._Animation_productions","🐰 Warner Bros. Toons — Films & TV","WBA","warner-bros-toons-films-tv",[0,1,4]),
    ("https://en.wikipedia.org/wiki/List_of_films_based_on_DC_Comics_publications","🌌 DC Comics: Live-Action Films","", "dc-comics-live-action-films",0),
]

QIDS = [
    ("Hunting Season", "Q137215321"),
    ("Turbulence", "Q137366793"),
    ("Batgirl", "Q109860581"),
    ("'Salem's Lot", "Q108552200"),
    ("100 Nights of Hero", "Q131290709"),
    ("101 Dalmatians II: Patch's London Adventure", "Q477365"),
    ("1408", "Q203560"),
    ("1922", "Q41156544"),
    ("28 Years Later", "Q125392328"),
    ("65", "Q104187039"),
    ("80 for Brady", "Q111941323"),
    ("825 Forest Road", "Q133885794"),
    ("A Big Bold Beautiful Journey", "Q124497778"),
    ("A Breed Apart", "Q133845943"),
    ("A Bug's Life", "Q216153"),
    ("A Complete Unknown", "Q118175825"),
    ("A Good Marriage", "Q18150326"),
    ("A Goofy Movie", "Q869993"),
    ("A Haunting in Venice", "Q114589148"),
    ("A House of Dynamite", "Q131686066"),
    ("A Journal for Jordan", "Q105560268"),
    ("A Little Prayer", "Q116954167"),
    ("A Man Called Otto", "Q111608782"),
    ("A Merry Little Ex-Mas", "Q132646702"),
    ("A Minecraft Movie", "Q40083273"),
    ("A Nice Indian Boy", "Q124255913"),
    ("A Quiet Place Part II", "Q53911403"),
    ("A Quiet Place: Day One", "Q112183404"),
    ("A Real Pain", "Q123690368"),
    ("A Return to Salem's Lot", "Q2628885"),
    ("A Spartan Dream", "Q136655065"),
    ("A Very Jonas Christmas Movie", "Q133502685"),
    ("A Working Man", "Q125495342"),
    ("Abigail", "Q120758628"),
    ("Abraham's Boys", "Q135221376"),
    ("Absolute Dominion", "Q115804585"),
    ("Adulthood", "Q125888131"),
    ("Adú", "Q83621079"),
    ("Afraid", "Q115859045"),
    ("After the Hunt", "Q127382372"),
    ("Afterburn", "Q124814444"),
    ("Aftershock: The Nicole P Bell Story", "Q137103837"),
    ("Aladdin", "Q215518"),
    ("Aladdin and the King of Thieves", "Q817003"),
    ("Alarum", "Q124979080"),
    ("Alexander and the Terrible, Horrible, No Good, Very Bad Road Trip", "Q125585498"),
    ("Alice in Wonderland", "Q189875"),
    ("Alien: Romulus", "Q115932544"),
    ("All My Life", "Q73543320"),
    ("All of Us Strangers", "Q113516747"),
    ("All of You", "Q129099102"),
    ("All That We Love", "Q136391118"),
    ("All the Devils Are Here", "Q135906912"),
    ("Alma and the Wolf", "Q125029935"),
    ("Altered", "Q131456643"),
    ("Ambulance", "Q105095144"),
    ("American Sweatshop", "Q131859890"),
    ("Americana", "Q117049250"),
    ("Amsterdam", "Q104830968"),
    ("An American Pickle", "Q58762343"),
    ("An Extremely Goofy Movie", "Q1537670"),
    ("Anaconda", "Q131549006"),
    ("Anemone", "Q130394023"),
    ("Anniversary", "Q120758667"),
    ("Another Simple Favor", "Q125371155"),
    ("Ant-Man", "Q5901134"),
    ("Ant-Man and the Wasp", "Q22957393"),
    ("Ant-Man and the Wasp: Quantumania", "Q105359456"),
    ("Antlers", "Q60737524"),
    ("Anyone but You", "Q118405167"),
    ("Apartment 7A", "Q112865585"),
    ("Appalachian Dog", "Q135067975"),
    ("Appendage", "Q124048896"),
    ("Apt Pupil", "Q570004"),
    ("Aquaman", "Q22998395"),
    ("Aquaman and the Lost Kingdom", "Q91226161"),
    ("Arco", "Q134571218"),
    ("Argylle", "Q107460063"),
    ("Armageddon Time", "Q108877975"),
    ("Around the World with Timon & Pumbaa", "Q18192535"),
    ("Artemis Fowl", "Q12302227"),
    ("Ash", "Q123862686"),
    ("Atlantis: Milo's Return", "Q629833"),
    ("Atlantis: The Lost Empire", "Q318975"),
    ("Atrabilious", "Q119819384"),
    ("Atropia", "Q124450737"),
    ("Avatar: Fire and Ash", "Q29580929"),
    ("Avatar: The Way of Water", "Q3604746"),
    ("Avengers: Age of Ultron", "Q14171368"),
    ("Avengers: Endgame", "Q23781155"),
    ("Avengers: Infinity War", "Q23780914"),
    ("Aztec Batman: Clash of Empires", "Q130267916"),
    ("Babes", "Q123144594"),
    ("Baby Invasion", "Q127716059"),
    ("Babylon", "Q107119206"),
    ("Back in Action", "Q116971739"),
    ("Bad Boys for Life", "Q29021224"),
    ("Bad Boys: Ride or Die", "Q31271357"),
    ("Bad Man", "Q125256466"),
    ("Bad Men Must Bleed", "Q136108816"),
    ("Bad Shabbos", "Q126492012"),
    ("Ballerina", "Q84713105"),
    ("Bambi", "Q43051"),
    ("Bambi II", "Q200889"),
    ("Barbarian", "Q38526439"),
    ("Batman", "Q810857"),
    ("Batman & Robin", "Q276523"),
    ("Batman Begins", "Q166262"),
    ("Batman Forever", "Q221345"),
    ("Batman Ninja vs. Yakuza League", "Q126680699"),
    ("Batman Returns", "Q189054"),
    ("Batman v Superman: Dawn of Justice", "Q14772351"),
    ("Batman: Mask of the Phantasm", "Q810858"),
    ("Bau: Artist at War", "Q135454923"),
    ("Beast", "Q111942495"),
    ("Beauty and the Beast", "Q179673"),
    ("Beauty and the Beast: The Enchanted Christmas", "Q678334"),
    ("Belfast", "Q100736066"),
    ("Belle's Magical World", "Q595300"),
    ("Belle's Tales of Friendship", "Q4883581"),
    ("Better Man", "Q106261265"),
    ("Better Nate Than Ever", "Q108830676"),
    ("Big Hero 6", "Q13091172"),
    ("Birds of Prey", "Q57177410"),
    ("Black Adam", "Q28657013"),
    ("Black Bag", "Q124373035"),
    ("Black Heat", "Q136654944"),
    ("Black Is King", "Q97516297"),
    ("Black Panther", "Q23780734"),
    ("Black Panther: Wakanda Forever", "Q54860489"),
    ("Black Phone 2", "Q124435493"),
    ("Black Widow", "Q23894626"),
    ("Blade", "Q3429263"),
    ("Blade II", "Q159638"),
    ("Blade: Trinity", "Q217008"),
    ("Blink", "Q130214464"),
    ("Bloat", "Q132859984"),
    ("Blood Star", "Q131386935"),
    ("Bloodshot", "Q56033171"),
    ("Blue Beetle", "Q107808351"),
    ("Blue Eyed Girl", "Q119059033"),
    ("Blue Moon", "Q127118129"),
    ("Bob Marley: One Love", "Q112341432"),
    ("Bob Trevino Likes It", "Q124844804"),
    ("Body Cam", "Q60737542"),
    ("Bolt", "Q212792"),
    ("Bone Lake", "Q135443289"),
    ("Book Club: The Next Chapter", "Q114798301"),
    ("Borderline", "Q130381570"),
    ("Boston Strangler", "Q110010607"),
    ("Both Eyes Open", "Q131870423"),
    ("Brave", "Q126796"),
    ("Brave the Dark", "Q123094814"),
    ("Brian and Charles", "Q110903725"),
    ("Bride Hard", "Q124289803"),
    ("Bridget Jones: Mad About the Boy", "Q124706420"),
    ("Broke", "Q125445898"),
    ("Bros", "Q110863967"),
    ("Brother Bear", "Q215365"),
    ("Brother Bear 2", "Q641310"),
    ("Bugonia", "Q125971387"),
    ("Bugs Bunny's 3rd Movie: 1001 Rabbit Tales", "Q1995292"),
    ("Bull Run", "Q133849169"),
    ("Bullet Train", "Q99900595"),
    ("Bunny", "Q136773157"),
    ("Buzz Lightyear of Star Command: The Adventure Begins", "Q1703163"),
    ("California King", "Q134136884"),
    ("Candyman", "Q64174907"),
    ("Captain America: Brave New World", "Q112322116"),
    ("Captain America: Civil War", "Q18407657"),
    ("Captain America: The First Avenger", "Q275120"),
    ("Captain America: The Winter Soldier", "Q1765358"),
    ("Captain Marvel", "Q23781129"),
    ("Carrie", "Q162672"),
    ("Cars", "Q182153"),
    ("Cars 2", "Q192212"),
    ("Cars 3", "Q21079862"),
    ("Cat's Eye", "Q1071567"),
    ("Cats Don't Dance", "Q930134"),
    ("Catwoman", "Q115760"),
    ("Caught Stealing", "Q130248726"),
    ("Cell", "Q17050502"),
    ("Champagne Problems", "Q131956860"),
    ("Chang Can Dunk", "Q108704293"),
    ("Chaperone", "Q124515188"),
    ("Charm City Kings", "Q58492592"),
    ("Cheaper by the Dozen", "Q110275950"),
    ("Checkmates", "Q123510271"),
    ("Chevalier", "Q108782357"),
    ("Chicken Little", "Q270940"),
    ("Children of the Corn", "Q467953"),
    ("Children of the Corn 666: Isaac's Return", "Q1502325"),
    ("Children of the Corn II: The Final Sacrifice", "Q467947"),
    ("Children of the Corn III: Urban Harvest", "Q368109"),
    ("Children of the Corn IV: The Gathering", "Q338012"),
    ("Children of the Corn V: Fields of Terror", "Q1548882"),
    ("Children of the Corn: Genesis", "Q2708890"),
    ("Children of the Corn: Revelation", "Q1072781"),
    ("Children of the Corn: Runaway", "Q56308839"),
    ("Chip 'n Dale: Rescue Rangers", "Q106075513"),
    ("Christine", "Q753449"),
    ("Christy", "Q130403642"),
    ("Cinderella", "Q83973483"),
    ("Cinderella II: Dreams Come True", "Q852391"),
    ("Cinderella III: A Twist in Time", "Q1092313"),
    ("Clifford the Big Red Dog", "Q65931068"),
    ("Clock", "Q117842589"),
    ("Clouds", "Q73549338"),
    ("Clown in a Cornfield", "Q127619022"),
    ("Cocaine Bear", "Q105840301"),
    ("Coco", "Q5815826"),
    ("Code 3", "Q123420921"),
    ("Cold Wallet", "Q124845026"),
    ("Coming 2 America", "Q64069446"),
    ("Companion", "Q125933907"),
    ("Constantine", "Q219150"),
    ("Control Freak", "Q133255152"),
    ("Coyotes", "Q136296182"),
    ("Crater", "Q107324772"),
    ("Creepshow", "Q744102"),
    ("Creepshow 2", "Q1139518"),
    ("Creepshow 3", "Q2639073"),
    ("Cruella", "Q24284363"),
    ("Cujo", "Q829996"),
    ("Cyrano", "Q104845839"),
    ("Daffy Duck's Fantastic Island", "Q5208329"),
    ("Daffy Duck's Quackbusters", "Q2081050"),
    ("Dangerous Animals", "Q133885811"),
    ("Darby and the Dead", "Q112160977"),
    ("Daredevil", "Q751805"),
    ("Dark Phoenix", "Q31188935"),
    ("Dashing Through the Snow", "Q27574320"),
    ("David", "Q136565787"),
    ("David Byrne's American Utopia", "Q97605240"),
    ("Day of Reckoning", "Q133809680"),
    ("DC League of Super-Pets", "Q106953528"),
    ("Dead of Winter", "Q124947655"),
    ("Deadpool", "Q19347291"),
    ("Deadpool & Wolverine", "Q102180106"),
    ("Deadpool 2", "Q25431158"),
    ("Dear Evan Hansen", "Q99368041"),
    ("Dear Santa", "Q117842560"),
    ("Death of a Unicorn", "Q120887939"),
    ("Death on the Nile", "Q56816969"),
    ("Deathstalker", "Q126008203"),
    ("Den of Thieves 2: Pantera", "Q121085761"),
    ("Descendent", "Q131860090"),
    ("Despicable Me 4", "Q113373276"),
    ("Devotion", "Q105962551"),
    ("Diablo", "Q124818558"),
    ("Diary of a Wimpy Kid", "Q108549760"),
    ("Diary of a Wimpy Kid Christmas: Cabin Fever", "Q122363694"),
    ("Diary of a Wimpy Kid: Rodrick Rules", "Q114842711"),
    ("Diary of a Wimpy Kid: The Last Straw", "Q136530239"),
    ("Die My Love", "Q117745793"),
    ("Dinosaur", "Q476329"),
    ("Disciples of the Crow", "Q4068107"),
    ("Disenchanted", "Q27121159"),
    ("Disney Princess Enchanted Tales: Follow Your Dreams", "Q3828818"),
    ("Doctor Sleep", "Q56881169"),
    ("Doctor Strange", "Q18406872"),
    ("Doctor Strange in the Multiverse of Madness", "Q64211112"),
    ("Dog Man", "Q124464619"),
    ("Doin' It", "Q124847512"),
    ("Dolan's Cadillac", "Q907787"),
    ("Dolittle", "Q38689704"),
    ("Dolores Claiborne", "Q1236347"),
    ("Dolphin Reef", "Q43746223"),
    ("Don't Tell Larry", "Q135785387"),
    ("Dora and the Search for Sol Dorado", "Q131197530"),
    ("Doug's 1st Movie", "Q2625713"),
    ("Downhill", "Q60737609"),
    ("Downton Abbey: The Grand Finale", "Q126164193"),
    ("Dreamcatcher", "Q1256231"),
    ("Dreaming of You", "Q136654956"),
    ("Drop", "Q125983478"),
    ("DuckTales the Movie: Treasure of the Lost Lamp", "Q1263501"),
    ("Dumb Money", "Q114740513"),
    ("Dumbo", "Q40895"),
    ("Dungeons & Dragons: Honor Among Thieves", "Q105554572"),
    ("Duplicity", "Q133465674"),
    ("Dust Bunny", "Q121572303"),
    ("East of Wall", "Q131963509"),
    ("Easter Sunday", "Q106706908"),
    ("Echo Valley", "Q119053380"),
    ("Eddington", "Q121433282"),
    ("Eden", "Q123472177"),
    ("Eenie Meanie", "Q130123022"),
    ("Eephus", "Q128635597"),
    ("Eileen", "Q113335768"),
    ("Eleanor the Great", "Q125627182"),
    ("Electra", "Q133573214"),
    ("Elektra", "Q610159"),
    ("Elemental", "Q112801489"),
    ("Elephant", "Q89411396"),
    ("Elio", "Q113879962"),
    ("Ella McCay", "Q124408474"),
    ("Elton John: Never Too Late", "Q127377886"),
    ("Emily the Criminal", "Q110039575"),
    ("Empire of Light", "Q110876878"),
    ("Encanto", "Q103372692"),
    ("Eric Larue", "Q114364101"),
    ("Escape Room: Tournament of Champions", "Q64027971"),
    ("Et Tu", "Q135484808"),
    ("Eternals", "Q23894629"),
    ("Eternity", "Q126203157"),
    ("Everything's Going to Be Great", "Q117846221"),
    ("Exit Protocol", "Q136722036"),
    ("Extremely Unique Dynamic", "Q126918442"),
    ("Eye for an Eye", "Q134457172"),
    ("F*** Marry Kill", "Q124150337"),
    ("F1", "Q114246242"),
    ("F9", "Q29466808"),
    ("Fackham Hall", "Q126004530"),
    ("Failure!", "Q121991068"),
    ("Fairyland", "Q115631713"),
    ("Familiar Touch", "Q130164694"),
    ("Fantasia", "Q943192"),
    ("Fantasia 2000", "Q30937"),
    ("Fantastic Four", "Q224130"),
    ("Fantastic Four: Rise of the Silver Surfer", "Q390063"),
    ("Fantasy Island", "Q60737563"),
    ("Fast X", "Q31271369"),
    ("Father Mother Sister Brother", "Q124364048"),
    ("Father Stu", "Q108031233"),
    ("Fatherhood", "Q65032686"),
    ("Fear Street Part One: 1994", "Q62066191"),
    ("Fear Street Part Three: 1666", "Q105373024"),
    ("Fear Street Part Two: 1978", "Q105369580"),
    ("Fear Street: Prom Queen", "Q125578001"),
    ("Fight or Flight", "Q131431712"),
    ("Final Destination Bloodlines", "Q118212751"),
    ("Finch", "Q55605492"),
    ("Finding Dory", "Q9321426"),
    ("Finding Joy", "Q136705825"),
    ("Finding Nemo", "Q132863"),
    ("Fire Island", "Q108101957"),
    ("Firestarter", "Q61883109"),
    ("Five Nights at Freddy's", "Q21512706"),
    ("Five Nights at Freddy's 2", "Q124216482"),
    ("Fixed", "Q117337620"),
    ("Flamin' Hot", "Q111915694"),
    ("Flight Risk", "Q123353613"),
    ("Flora & Ulysses", "Q83952365"),
    ("Fly Me to the Moon", "Q115057826"),
    ("Fountain of Youth", "Q124256460"),
    ("Frankenstein", "Q124393045"),
    ("Freakier Friday", "Q126897811"),
    ("Freaky", "Q73536996"),
    ("Freaky Tales", "Q115804877"),
    ("Free Guy", "Q38685497"),
    ("Fresh", "Q105393880"),
    ("Friendship", "Q127821688"),
    ("Frontier Crucible", "Q131308619"),
    ("Frozen", "Q246283"),
    ("Frozen 2", "Q24733929"),
    ("Fuck My Son!", "Q136352638"),
    ("Full of Grace", "Q113101412"),
    ("Fun and Fancy Free", "Q853718"),
    ("G20", "Q124337176"),
    ("Gabby's Dollhouse: The Movie", "Q126414863"),
    ("Gargoyles the Movie: The Heroes Awaken", "Q2718148"),
    ("Gazer", "Q126281957"),
    ("Genie", "Q123271679"),
    ("Gerald's Game", "Q27940451"),
    ("Ghost Rider", "Q41754"),
    ("Ghost Rider: Spirit of Vengeance", "Q41854"),
    ("Ghostbusters: Afterlife", "Q61883088"),
    ("Ghostbusters: Frozen Empire", "Q113300586"),
    ("Gladiator II", "Q115789958"),
    ("Godmothered", "Q83726410"),
    ("Good Bad Things", "Q127763750"),
    ("Good Boy", "Q133427678"),
    ("Good Fortune", "Q124364757"),
    ("Good Luck to You, Leo Grande", "Q106740037"),
    ("Good on Paper", "Q107071324"),
    ("Goodbye June", "Q133311073"),
    ("Gran Turismo", "Q112909147"),
    ("Graveyard Shift", "Q944115"),
    ("Green and Gold", "Q132125859"),
    ("Green Lantern", "Q903885"),
    ("Griffin in Summer", "Q125506620"),
    ("Grimcutty", "Q114812849"),
    ("Guardians of the Galaxy", "Q5887360"),
    ("Guardians of the Galaxy Vol. 2", "Q20001199"),
    ("Guardians of the Galaxy Vol. 3", "Q29226331"),
    ("Guns & Moses", "Q127384884"),
    ("Guns Up", "Q133849869"),
    ("Gunslingers", "Q133055066"),
    ("Hallow Road", "Q124749841"),
    ("Halloween Ends", "Q85997053"),
    ("Halloween Kills", "Q67279679"),
    ("Hamilton", "Q84323848"),
    ("Hamnet", "Q122741016"),
    ("Happy Gilmore 2", "Q130269576"),
    ("Harold and the Purple Crayon", "Q111883640"),
    ("Haunted Mansion", "Q108940282"),
    ("Havoc", "Q107584295"),
    ("Heads of State", "Q118364834"),
    ("Heart Eyes", "Q127163221"),
    ("Hearts in Atlantis", "Q780434"),
    ("Hedda", "Q124215661"),
    ("Hell House LLC: Lineage", "Q131548051"),
    ("Hell of a Summer", "Q119111677"),
    ("Henry Danger: The Movie", "Q124841935"),
    ("Hercules", "Q271189"),
    ("Hercules: Zero to Hero", "Q1609593"),
    ("Here After", "Q133820360"),
    ("High Rollers", "Q133141256"),
    ("Highest 2 Lowest", "Q124512337"),
    ("Him", "Q130410549"),
    ("Hocus Pocus 2", "Q109284173"),
    ("Hold Your Breath", "Q113757799"),
    ("Holland", "Q116730669"),
    ("Hollywood Stargirl", "Q107121729"),
    ("Holy Ghost", "Q135112482"),
    ("Home on the Range", "Q936194"),
    ("Home Sweet Home Alone", "Q102046208"),
    ("Home Sweet Home Rebirth", "Q123468575"),
    ("Honey Don't!", "Q124352605"),
    ("Hotel Transylvania: Transformania", "Q65973230"),
    ("House of Gucci", "Q105549749"),
    ("How to Train Your Dragon", "Q118904382"),
    ("Howard the Duck", "Q1146570"),
    ("Hulk", "Q696042"),
    ("Hurry Up Tomorrow", "Q122190665"),
    ("I Don't Understand You", "Q124537941"),
    ("I Know What You Did Last Summer", "Q130368011"),
    ("I Love You Forever", "Q124847605"),
    ("I Wish You All the Best", "Q124253867"),
    ("I'm Beginning to See the Light", "Q131524454"),
    ("Ice Road: Vengeance", "Q124323597"),
    ("Icefall", "Q125627194"),
    ("Ick", "Q133281186"),
    ("IF", "Q111918059"),
    ("If I Had Legs I'd Kick You", "Q124450734"),
    ("If These Walls Could Sing", "Q115517341"),
    ("In Our Blood", "Q128259171"),
    ("In the Lost Lands", "Q113524284"),
    ("In the Tall Grass", "Q56241212"),
    ("In Your Dreams", "Q133261063"),
    ("Incredibles 2", "Q24832112"),
    ("Indiana Jones and the Dial of Destiny", "Q24278982"),
    ("Infinite", "Q64067920"),
    ("Influencers", "Q135226835"),
    ("Inheritance", "Q131348310"),
    ("Inside Out", "Q6144664"),
    ("Inside Out 2", "Q113877606"),
    ("Into the Deep", "Q131844396"),
    ("Iron Man", "Q192724"),
    ("Iron Man 2", "Q205028"),
    ("Iron Man 3", "Q209538"),
    ("Is This Thing On?", "Q132189965"),
    ("It", "Q25136484"),
    ("It Chapter Two", "Q42726338"),
    ("It Ends", "Q133546293"),
    ("It Ends with Us", "Q118641054"),
    ("Jackass Forever", "Q104528612"),
    ("Jagged Mind", "Q118958500"),
    ("Jay Kelly", "Q124829697"),
    ("Jazzy", "Q125507012"),
    ("Jerry & Marge Go Large", "Q107739693"),
    ("Jim Henson Idea Man", "Q125448412"),
    ("Jimmy and Stiggs", "Q136655063"),
    ("Jingle Bell Heist", "Q123910761"),
    ("Joker", "Q42759035"),
    ("Joker: Folie à Deux", "Q108628759"),
    ("Jonah Hex", "Q596085"),
    ("Joy to the World", "Q136682442"),
    ("Juliet & Romeo", "Q132146913"),
    ("Jungle Cruise", "Q24285605"),
    ("Jungle Cubs: Born to Be Wild", "Q136513480"),
    ("Jurassic World Dominion", "Q55178974"),
    ("Jurassic World Rebirth", "Q124380480"),
    ("Justice League", "Q20501835"),
    ("Karate Kid: Legends", "Q124246470"),
    ("Keeper", "Q130753385"),
    ("Killer Rental", "Q136822226"),
    ("Killers of the Flower Moon", "Q66316924"),
    ("Killing Faith", "Q126095340"),
    ("Kinda Pregnant", "Q125621796"),
    ("Kinds of Kindness", "Q114345051"),
    ("King Ivory", "Q123986303"),
    ("Kingdom of the Planet of the Apes", "Q114314695"),
    ("Kiss of the Spider Woman", "Q125078429"),
    ("Knock at the Cabin", "Q111684978"),
    ("KPop Demon Hunters", "Q130268157"),
    ("Kraven the Hunter", "Q107390449"),
    ("Kronk's New Groove", "Q937486"),
    ("Kung Fu Panda 4", "Q114707487"),
    ("La Dolce Villa", "Q125532822"),
    ("Laal Singh Chaddha", "Q73093255"),
    ("Lady and the Tramp", "Q172284"),
    ("Lady and the Tramp II: Scamp's Adventure", "Q244434"),
    ("Last Breath", "Q119225271"),
    ("Last Days", "Q125962650"),
    ("Laws of Man", "Q131870359"),
    ("Left-Handed Girl", "Q133871355"),
    ("Let Them All Talk", "Q66828800"),
    ("Licorice Pizza", "Q102036245"),
    ("Lifeline", "Q135987705"),
    ("Light of the World", "Q135967774"),
    ("Lightyear", "Q104409060"),
    ("Like a Boss", "Q57779949"),
    ("Like Father Like Son", "Q133747349"),
    ("Lilly", "Q130287348"),
    ("Lilo & Stitch", "Q59395971"),
    ("Lilo & Stitch 2: Stitch Has a Glitch", "Q1122127"),
    ("Locked", "Q124370375"),
    ("Locked-in Society", "Q111512798"),
    ("Logan", "Q24053263"),
    ("London Calling", "Q124652747"),
    ("Long Distance", "Q104861381"),
    ("Long Shadows", "Q135975526"),
    ("Looking Through Water", "Q119975269"),
    ("Looney Tunes: Back in Action", "Q834165"),
    ("Love and Monsters", "Q62761126"),
    ("Love Hurts", "Q125265183"),
    ("Love Me", "Q123690366"),
    ("Love, Brooklyn", "Q131428647"),
    ("Luca", "Q97925311"),
    ("Lurker", "Q131428108"),
    ("Lyle, Lyle, Crocodile", "Q108559256"),
    ("M3GAN", "Q110661062"),
    ("M3GAN 2.0", "Q116258185"),
    ("Madame Web", "Q111677117"),
    ("Madea's Destination Wedding", "Q124397410"),
    ("Madu", "Q135114734"),
    ("Magazine Dreams", "Q115631553"),
    ("Magic Camp", "Q27685110"),
    ("Magic Farm", "Q131428110"),
    ("Maintenance Required", "Q136311778"),
    ("Make Mine Music", "Q754736"),
    ("Man Finds Tape", "Q137215326"),
    ("Man of Steel", "Q622769"),
    ("Man with No Past", "Q131870380"),
    ("Marked Men: Rule + Shaw", "Q131419946"),
    ("Marry Me", "Q73543375"),
    ("Marty Supreme", "Q130118681"),
    ("Materialists", "Q125726790"),
    ("Matriarch", "Q115474904"),
    ("Maximum Overdrive", "Q46637"),
    ("McVeigh", "Q125585147"),
    ("Mean Girls", "Q117014906"),
    ("Meet the Robinsons", "Q221679"),
    ("Melody Time", "Q869741"),
    ("Mercy", "Q16664100"),
    ("Merrily We Roll Along", "Q137215299"),
    ("Merry Little Batman", "Q123466026"),
    ("Merv", "Q125825858"),
    ("Messy", "Q136677648"),
    ("Mickey 17", "Q112977213"),
    ("Mickey's House of Villains", "Q1630767"),
    ("Mickey's Magical Christmas: Snowed in at the House of Mouse", "Q656396"),
    ("Mickey's Once Upon a Christmas", "Q1655222"),
    ("Mickey's Twice Upon a Christmas", "Q1203831"),
    ("Mickey, Donald, Goofy: The Three Musketeers", "Q1150785"),
    ("Mickey: The Story of a Mouse", "Q111943849"),
    ("Mighty Ducks the Movie: The First Face-Off", "Q3312846"),
    ("Mighty Oak", "Q98171625"),
    ("Migration", "Q117457809"),
    ("Mija", "Q136513180"),
    ("Millers in Marriage", "Q128942948"),
    ("Minions: The Rise of Gru", "Q60997779"),
    ("Misery", "Q725552"),
    ("Mission: Impossible – Dead Reckoning Part One", "Q61876370"),
    ("Mission: Impossible – The Final Reckoning", "Q61876374"),
    ("Moana", "Q18647981"),
    ("Moana 2", "Q124457266"),
    ("Monkey Man", "Q105952623"),
    ("Monsters University", "Q641362"),
    ("Monsters, Inc.", "Q187726"),
    ("Moonage Daydream", "Q112454289"),
    ("Morbius", "Q55405369"),
    ("More Than Robots", "Q136513288"),
    ("Motherland", "Q136655081"),
    ("Mountainhead", "Q133750993"),
    ("Mr. Harrigan's Phone", "Q111594409"),
    ("Mr. Malcolm's List", "Q106091227"),
    ("Mrs. Harris Goes to Paris", "Q104871482"),
    ("Mufasa: The Lion King", "Q114242678"),
    ("Mulan", "Q24284283"),
    ("Mulan II", "Q837847"),
    ("Murder at the Embassy", "Q131523345"),
    ("Music by John Williams", "Q130394862"),
    ("Muzzle: City of Wolves", "Q136432677"),
    ("My Animal", "Q116769259"),
    ("My Big Fat Greek Wedding 3", "Q117444564"),
    ("My Dead Friend Zoe", "Q124250689"),
    ("My Mother's Wedding", "Q115517901"),
    ("My Oxford Year", "Q130383564"),
    ("My Secret Santa", "Q135969889"),
    ("Napoleon", "Q105806948"),
    ("Narcissus and Goldmund", "Q38685846"),
    ("Needful Things", "Q1660749"),
    ("Neighborhood Watch", "Q133808367"),
    ("News of the World", "Q73537408"),
    ("Next Goal Wins", "Q73537958"),
    ("Night Always Comes", "Q126077186"),
    ("Night at the Museum: Kahmunrah Rises Again", "Q112723916"),
    ("Night Swim", "Q117842595"),
    ("Nightbitch", "Q115475553"),
    ("Nightmare Alley", "Q82430214"),
    ("No Address", "Q119625134"),
    ("No Exit", "Q107524848"),
    ("No Hard Feelings", "Q114397511"),
    ("No One Will Save You", "Q122231675"),
    ("No Smoking", "Q3877535"),
    ("No Time to Die", "Q21534241"),
    ("Nobody", "Q83965418"),
    ("Nobody 2", "Q126682448"),
    ("Nomadland", "Q61740820"),
    ("Nonnas", "Q120881724"),
    ("Nope", "Q107176162"),
    ("Nosferatu", "Q116361132"),
    ("Not Okay", "Q108123132"),
    ("Not Without Hope", "Q123137223"),
    ("Nouvelle Vague", "Q125867250"),
    ("Novocaine", "Q124707042"),
    ("Now You See Me: Now You Don't", "Q65146722"),
    ("Nuremberg", "Q124393313"),
    ("O'Dessa", "Q125421437"),
    ("Off the Grid", "Q131198881"),
    ("Oh, Hi!", "Q131429514"),
    ("Oh. What. Fun.", "Q125758998"),
    ("Old", "Q99688807"),
    ("Old Guy", "Q117843038"),
    ("Oliver & Company", "Q216929"),
    ("On Becoming a Guinea Fowl", "Q125457216"),
    ("On Swift Horses", "Q116971312"),
    ("On the Come Up", "Q113514946"),
    ("One Battle After Another", "Q117085614"),
    ("One Hundred and One Dalmatians", "Q165512"),
    ("One Night in Tokyo", "Q136654923"),
    ("One of Them Days", "Q130341202"),
    ("Onward", "Q59690895"),
    ("Oppenheimer", "Q108839994"),
    ("Opus", "Q123754013"),
    ("Oracle", "Q106652079"),
    ("Organ Trail", "Q124248924"),
    ("Orphan: First Kill", "Q101229556"),
    ("Osiris", "Q124625067"),
    ("Osmosis Jones", "Q966690"),
    ("Outerlands", "Q132529713"),
    ("Paddington in Peru", "Q121076570"),
    ("Paranormal Activity: Next of Kin", "Q108619707"),
    ("Pavements", "Q115866950"),
    ("Paw Patrol: The Mighty Movie", "Q111918018"),
    ("Paw Patrol: The Movie", "Q97609238"),
    ("Paws of Fury: The Legend of Hank", "Q21450513"),
    ("Pearl", "Q111669794"),
    ("People Just Do Nothing: Big in Japan", "Q76890341"),
    ("Pet Sematary", "Q908213"),
    ("Pet Sematary Two", "Q1079636"),
    ("Pet Sematary: Bloodlines", "Q120733524"),
    ("Peter Hujar's Day", "Q131428368"),
    ("Peter Pan", "Q270470"),
    ("Peter Pan & Wendy", "Q28973277"),
    ("Peter Rabbit 2: The Runaway", "Q61951873"),
    ("Phineas and Ferb the Movie: Candace Against the Universe", "Q66725868"),
    ("Piece by Piece", "Q124364761"),
    ("Piglet's Big Movie", "Q1406374"),
    ("Pinocchio", "Q105482844"),
    ("Pixie", "Q73898752"),
    ("Plainclothes", "Q125102928"),
    ("Planes", "Q1657080"),
    ("Planes: Fire & Rescue", "Q15631322"),
    ("Plankton: The Movie", "Q126453844"),
    ("Play Dirty", "Q124353875"),
    ("Playdate", "Q126209905"),
    ("Please Don't Destroy: The Treasure of Foggy Mountain", "Q123116668"),
    ("Please Don't Feed the Children", "Q121572304"),
    ("Pocahontas", "Q218894"),
    ("Pocahontas II: Journey to a New World", "Q1349588"),
    ("Polar Bear", "Q111720319"),
    ("Ponyboi", "Q123693164"),
    ("Pooh's Grand Adventure: The Search for Christopher Robin", "Q919563"),
    ("Pooh's Heffalump Halloween Movie", "Q2036064"),
    ("Pooh's Heffalump Movie", "Q1361113"),
    ("Pools", "Q135967777"),
    ("Poor Things", "Q108760382"),
    ("Popeye the Slayer Man", "Q132817663"),
    ("Pose", "Q123384631"),
    ("Praise This", "Q117842581"),
    ("Predator: Badlands", "Q126396314"),
    ("Predator: Killer of Killers", "Q130639284"),
    ("Preparation for the Next Life", "Q135404716"),
    ("Presence", "Q123690382"),
    ("Pretty Thing", "Q134540616"),
    ("Prey", "Q108329738"),
    ("Prime Minister", "Q131978272"),
    ("Prisoner of War", "Q133202863"),
    ("Profile", "Q48673984"),
    ("Project MKHEXE", "Q134173771"),
    ("Psycho Therapy: The Shallow Tale of a Writer Who Decided to Write About a Serial Killer", "Q123263821"),
    ("Punisher: War Zone", "Q1065829"),
    ("Puss in Boots: The Last Wish", "Q31271384"),
    ("Quasi", "Q115426515"),
    ("Queen of Bones", "Q124556281"),
    ("Queen of the Ring", "Q125523347"),
    ("Queens of the Dead", "Q130643824"),
    ("Quest for Camelot", "Q1764174"),
    ("Quiz Lady", "Q113635014"),
    ("Rabbit Trap", "Q123260727"),
    ("Ralph Breaks the Internet", "Q25167044"),
    ("Ratatouille", "Q170035"),
    ("Raya and the Last Dragon", "Q66738105"),
    ("Re-Election", "Q136565818"),
    ("Rebuilding", "Q131428292"),
    ("Recess Christmas: Miracle on Third Street", "Q4357623"),
    ("Recess: All Growed Down", "Q7302373"),
    ("Recess: School's Out", "Q969270"),
    ("Recess: Taking the Fifth Grade", "Q5412173"),
    ("Red Sonja", "Q113638950"),
    ("Redeeming Love", "Q97182477"),
    ("Regretting You", "Q135907051"),
    ("Relay", "Q117742969"),
    ("Renfield", "Q110811753"),
    ("Renner", "Q130244395"),
    ("Rental Family", "Q124947229"),
    ("Respect", "Q79327081"),
    ("Return to Never Land", "Q610262"),
    ("Reverence", "Q136891758"),
    ("Riding the Bullet", "Q2151916"),
    ("Riff Raff", "Q123511612"),
    ("Riley", "Q124248117"),
    ("Rise", "Q109285540"),
    ("Robin Hood", "Q19090"),
    ("Ron's Gone Wrong", "Q106299119"),
    ("Roofman", "Q130742503"),
    ("Rosaline", "Q16673195"),
    ("Rosario", "Q132672654"),
    ("Rosemead", "Q133889047"),
    ("Round the Decay", "Q131937855"),
    ("Rounding", "Q136368828"),
    ("Ruby Gillman, Teenage Kraken", "Q117188308"),
    ("Rule Breakers", "Q133172925"),
    ("Rumble", "Q83952024"),
    ("Run", "Q131226672"),
    ("Rust", "Q109051190"),
    ("Ruth & Boaz", "Q135924160"),
    ("Rye Lane", "Q115675219"),
    ("Sacramento", "Q119149787"),
    ("Safe House", "Q136238688"),
    ("Safety", "Q73897634"),
    ("Saint Clare", "Q123125161"),
    ("Saludos Amigos", "Q842306"),
    ("Sanatorium Under the Sign of the Hourglass", "Q135618165"),
    ("Sanctuary", "Q111211523"),
    ("Sarah's Oil", "Q130693545"),
    ("Saturday Night", "Q124785550"),
    ("Savage Hunt", "Q137186379"),
    ("Saving Buddy Charles", "Q137397643"),
    ("Scoob!", "Q23999279"),
    ("Scream", "Q88563268"),
    ("Scream VI", "Q112183099"),
    ("Screamboat", "Q124131345"),
    ("Sea Lions of the Galapagos", "Q134055603"),
    ("Seberg", "Q55603330"),
    ("Secret Headquarters", "Q106994644"),
    ("Secret of the Wings", "Q1702819"),
    ("Secret Window", "Q472165"),
    ("See How They Run", "Q105319897"),
    ("Self-Help", "Q136673337"),
    ("Senior Year", "Q107639384"),
    ("September 5", "Q127688495"),
    ("Sew Torn", "Q124853345"),
    ("Shadow Force", "Q119625137"),
    ("Shang-Chi and the Legend of the Ten Rings", "Q65768589"),
    ("Shazam!", "Q23685878"),
    ("Shazam! Fury of the Gods", "Q84712809"),
    ("She Came to Me", "Q116172540"),
    ("She Rides Shotgun", "Q124464617"),
    ("She Said", "Q107969593"),
    ("Shelby Oaks", "Q112039665"),
    ("Shell", "Q124214428"),
    ("Shooting Stars", "Q118978053"),
    ("Significant Other", "Q113977063"),
    ("Silent Night, Deadly Night", "Q134386480"),
    ("Silver Bullet", "Q1198033"),
    ("Sincerely Saul", "Q135957624"),
    ("Sing 2", "Q30933418"),
    ("Sinners", "Q125473145"),
    ("Sisu: Road to Revenge", "Q125020375"),
    ("Site", "Q135902002"),
    ("Sketch", "Q130276120"),
    ("Skillhouse", "Q135209236"),
    ("Sleeping Beauty", "Q215617"),
    ("Smallfoot", "Q30607974"),
    ("Smile", "Q112623225"),
    ("Smile 2", "Q123472063"),
    ("Smurfs", "Q124125148"),
    ("Snake Eyes: G.I. Joe Origins", "Q64027609"),
    ("Sneaks", "Q131318882"),
    ("Snorkeling", "Q116244091"),
    ("Snow White", "Q27660145"),
    ("Snow White and the Seven Dwarfs", "Q134430"),
    ("Sometimes They Come Back... Again", "Q605613"),
    ("Sometimes They Come Back... for More", "Q4201255"),
    ("Song Sung Blue", "Q130532687"),
    ("Sonic the Hedgehog", "Q29906232"),
    ("Sonic the Hedgehog 2", "Q105970894"),
    ("Sonic the Hedgehog 3", "Q115859490"),
    ("Sorry, Baby", "Q131450483"),
    ("Soul", "Q64744044"),
    ("Soul on Fire", "Q124098394"),
    ("Sovereign", "Q124737122"),
    ("Space Jam", "Q207659"),
    ("Space Jam: A New Legacy", "Q56850065"),
    ("Speak No Evil", "Q119064873"),
    ("Speed Train", "Q137215349"),
    ("Spell", "Q101094463"),
    ("Spider-Man", "Q484442"),
    ("Spider-Man 2", "Q190145"),
    ("Spider-Man 3", "Q182212"),
    ("Spider-Man: Across the Spider-Verse", "Q76448600"),
    ("Spider-Man: Far From Home", "Q27985819"),
    ("Spider-Man: Homecoming", "Q23010088"),
    ("Spider-Man: No Way Home", "Q68934496"),
    ("Spinal Tap II: The End Continues", "Q121088318"),
    ("Spirit Untamed", "Q97289392"),
    ("Splitsville", "Q131004938"),
    ("Spontaneous", "Q60738091"),
    ("Springsteen: Deliver Me from Nowhere", "Q125401070"),
    ("Stand by Me", "Q494722"),
    ("Star People", "Q136655054"),
    ("Star Trek: Section 31", "Q106948764"),
    ("Stargirl", "Q57474414"),
    ("Stealing Pulp Fiction", "Q130484589"),
    ("Steel", "Q1790253"),
    ("Stitch! The Movie", "Q1468857"),
    ("Stolen Girl", "Q136386270"),
    ("Stone Cold Fox", "Q126902066"),
    ("Stone Creek Killer", "Q137103841"),
    ("Storks", "Q21450480"),
    ("Strange Harvest", "Q130379751"),
    ("Strange World", "Q107473453"),
    ("Straw", "Q134300783"),
    ("Strays", "Q111340524"),
    ("Suicide Squad", "Q18604504"),
    ("Summer of 69", "Q130287347"),
    ("Summer of Soul", "Q104213699"),
    ("Suncoast", "Q119882908"),
    ("Sunfish (& Other Stories on Green Lake)", "Q131447604"),
    ("Supergirl", "Q283134"),
    ("Superintelligence", "Q55696425"),
    ("Superman", "Q116547790"),
    ("Superman and the Mole Men", "Q1115677"),
    ("Superman II", "Q267672"),
    ("Superman III", "Q528095"),
    ("Superman IV: The Quest for Peace", "Q1123006"),
    ("Superman Returns", "Q328695"),
    ("Swamp Thing", "Q1810512"),
    ("Sweet Dreams", "Q126897786"),
    ("Swiped", "Q125966692"),
    ("Tad, the Lost Explorer and the Emerald Tablet", "Q113459376"),
    ("Tales from the Darkside: The Movie", "Q387603"),
    ("Tangled", "Q188439"),
    ("Tarzan", "Q208696"),
    ("Tarzan & Jane", "Q2031788"),
    ("Tarzan II", "Q258815"),
    ("Tatami", "Q118640057"),
    ("Taz: Quest for Burger", "Q121094685"),
    ("Teacher's Pet", "Q1229070"),
    ("Teen Titans Go! To the Movies", "Q46392313"),
    ("Teenage Mutant Ninja Turtles: Mutant Mayhem", "Q110271518"),
    ("Tenet", "Q63985561"),
    ("The 355", "Q65090801"),
    ("The A-Frame", "Q126184078"),
    ("The Accidental Getaway Driver", "Q116736286"),
    ("The Accountant 2", "Q124848369"),
    ("The Actor", "Q116498761"),
    ("The Addams Family 2", "Q100292318"),
    ("The Adults", "Q116765532"),
    ("The Adventures of Ichabod and Mr. Toad", "Q863963"),
    ("The Alto Knights", "Q115805392"),
    ("The Amateur", "Q119206814"),
    ("The Amazing Spider-Man", "Q229808"),
    ("The Amazing Spider-Man 2", "Q4254026"),
    ("The American Society of Magical Negroes", "Q123690415"),
    ("The Aristocats", "Q184701"),
    ("The Assessment", "Q126086644"),
    ("The Astronaut", "Q125620071"),
    ("The Avengers", "Q182218"),
    ("The Bad Guys", "Q97288428"),
    ("The Bad Guys 2", "Q125363929"),
    ("The Baltimorons", "Q133041175"),
    ("The Banshees of Inisherin", "Q108314506"),
    ("The Batman", "Q61117344"),
    ("The Beach Boys", "Q125400786"),
    ("The Beatles: Get Back - The Rooftop Concert", "Q110787453"),
    ("The Beldham", "Q131289297"),
    ("The Best You Can", "Q133890071"),
    ("The Big Bend", "Q136654951"),
    ("The Bikeriders", "Q113668560"),
    ("The Black Cauldron", "Q329316"),
    ("The Black Phone", "Q105584511"),
    ("The Bob's Burgers Movie", "Q41709271"),
    ("The Boogeyman", "Q111530175"),
    ("The Boss Baby: Family Business", "Q11640531"),
    ("The Buildout", "Q132322748"),
    ("The Call of the Wild", "Q57982258"),
    ("The Carpenter's Son", "Q125911900"),
    ("The Christmas Ring", "Q136411457"),
    ("The Chronology of Water", "Q116776314"),
    ("The Conjuring: Last Rites", "Q114802043"),
    ("The Contractor", "Q73549328"),
    ("The Craft: Legacy", "Q74191011"),
    ("The Creator", "Q110617876"),
    ("The Croods: A New Age", "Q24832743"),
    ("The Cut", "Q128774983"),
    ("The Damned", "Q124426106"),
    ("The Dark Half", "Q940066"),
    ("The Dark Knight", "Q163872"),
    ("The Dark Knight Rises", "Q189330"),
    ("The Dark Tower", "Q21647114"),
    ("The Day the Earth Blew Up: A Looney Tunes Movie", "Q123234434"),
    ("The Dead Thing", "Q130615106"),
    ("The Dead Zone", "Q466792"),
    ("The Death of Film", "Q136654895"),
    ("The Death of Snow White", "Q133885795"),
    ("The Devil and the Daylong Brothers", "Q133747736"),
    ("The Diary of Ellen Rimbauer", "Q1170504"),
    ("The Electric State", "Q114492769"),
    ("The Emperor's New Groove", "Q223163"),
    ("The Empty Man", "Q39069693"),
    ("The Equalizer 3", "Q114818951"),
    ("The Exorcist: Believer", "Q107610336"),
    ("The Eyes of Tammy Faye", "Q73537423"),
    ("The Fabelmans", "Q107710009"),
    ("The Fall Guy", "Q113671585"),
    ("The Family McMullen", "Q136428304"),
    ("The Family Plan 2", "Q132682968"),
    ("The Fantastic Four: First Steps", "Q105105219"),
    ("The Fetus", "Q136655074"),
    ("The First Omen", "Q117842576"),
    ("The Flash", "Q23562553"),
    ("The Forever Purge", "Q64067279"),
    ("The Forgiven", "Q85807206"),
    ("The Fox and the Hound", "Q40302"),
    ("The Fox and the Hound 2", "Q188899"),
    ("The French Dispatch", "Q59551547"),
    ("The Friend", "Q124853968"),
    ("The Garfield Movie", "Q113547547"),
    ("The Good Dinosaur", "Q7737199"),
    ("The Gorge", "Q116971304"),
    ("The Great Mouse Detective", "Q334132"),
    ("The Greatest Hits", "Q117272587"),
    ("The Green Mile", "Q208263"),
    ("The Hand That Rocks the Cradle", "Q133627078"),
    ("The Harvest", "Q133814834"),
    ("The History of Sound", "Q118765520"),
    ("The Home", "Q123050485"),
    ("The Housemaid", "Q131630491"),
    ("The Hunchback of Notre Dame", "Q213787"),
    ("The Hunchback of Notre Dame II", "Q1193937"),
    ("The Hunt", "Q62720813"),
    ("The Ice Age Adventures of Buck Wild", "Q105992748"),
    ("The In Between", "Q110903689"),
    ("The Incredible Hulk", "Q466611"),
    ("The Incredibles", "Q213326"),
    ("The Invisible Man", "Q64004122"),
    ("The Iron Giant", "Q867283"),
    ("The Jungle Book", "Q199839"),
    ("The Jungle Book 2", "Q221947"),
    ("The Killer", "Q127257393"),
    ("The King of Kings", "Q131382798"),
    ("The King of Staten Island", "Q64338076"),
    ("The King's Man", "Q61633664"),
    ("The Knife", "Q126326956"),
    ("The Last Duel", "Q85000980"),
    ("The Last Journey", "Q123857797"),
    ("The Last Rodeo", "Q131632909"),
    ("The Last Supper", "Q132716707"),
    ("The Last Voyage of the Demeter", "Q107394970"),
    ("The Legend of Ochi", "Q117336433"),
    ("The Lego Batman Movie", "Q23013169"),
    ("The Lego Movie", "Q2608065"),
    ("The Lego Movie 2: The Second Part", "Q23796643"),
    ("The Lego Ninjago Movie", "Q22131901"),
    ("The Life List", "Q130383565"),
    ("The Life of Chuck", "Q123169098"),
    ("The Lion King", "Q36479"),
    ("The Lion King 1½", "Q223217"),
    ("The Lion King II: Simba's Pride", "Q191803"),
    ("The Little Mermaid", "Q24357415"),
    ("The Little Mermaid II: Return to the Sea", "Q467678"),
    ("The Little Mermaid: Ariel's Beginning", "Q659725"),
    ("The Long Game", "Q136654911"),
    ("The Long Walk", "Q126511469"),
    ("The Looney Looney Looney Bugs Bunny Movie", "Q3836716"),
    ("The Lord of the Rings: The War of the Rohirrim", "Q107210110"),
    ("The Lost Bus", "Q125153299"),
    ("The Lost City", "Q107040798"),
    ("The Lovebirds", "Q61782227"),
    ("The Luckiest Man in America", "Q125861561"),
    ("The Magic Kids: Three Unlikely Heroes", "Q78260411"),
    ("The Man from Toronto", "Q101091774"),
    ("The Man in My Basement", "Q124528247"),
    ("The Mangler", "Q1660446"),
    ("The Mangler 2", "Q1513631"),
    ("The Mangler Reborn", "Q3283155"),
    ("The Many Adventures of Winnie the Pooh", "Q498434"),
    ("The Map That Leads to You", "Q131760056"),
    ("The Marvels", "Q89474225"),
    ("The Mastermind", "Q130569520"),
    ("The Menu", "Q108444804"),
    ("The Mill", "Q122945078"),
    ("The Mist", "Q695209"),
    ("The Mitchells vs. the Machines", "Q54021143"),
    ("The Monkey", "Q124841936"),
    ("The Mortimers", "Q135436742"),
    ("The Mother, the Menacer, and Me", "Q137352527"),
    ("The Naked Gun", "Q125908027"),
    ("The New Mutants", "Q28912376"),
    ("The Night Flier", "Q2060440"),
    ("The Night House", "Q77906196"),
    ("The Northman", "Q87126244"),
    ("The Occupant", "Q135726724"),
    ("The Old Guard 2", "Q114352640"),
    ("The One and Only Ivan", "Q54958420"),
    ("The Parenting", "Q111941115"),
    ("The Perfect Gamble", "Q130569705"),
    ("The Personal History of David Copperfield", "Q54489141"),
    ("The Phantom of the Open", "Q104863935"),
    ("The Phoenician Scheme", "Q124758337"),
    ("The Photograph", "Q63728411"),
    ("The Pickup", "Q125578089"),
    ("The Pirate Fairy", "Q15396069"),
    ("The Plague", "Q134052834"),
    ("The Princess", "Q111942703"),
    ("The Princess and the Frog", "Q171300"),
    ("The Punisher", "Q909802"),
    ("The Rage: Carrie 2", "Q532461"),
    ("The Rescuers", "Q39722"),
    ("The Rescuers Down Under", "Q202371"),
    ("The Return of Jafar", "Q817261"),
    ("The Return of Swamp Thing", "Q2521650"),
    ("The Rhythm Section", "Q47063439"),
    ("The Ritual", "Q125578090"),
    ("The Roses", "Q126810973"),
    ("The Royal Hotel", "Q120887436"),
    ("The Running Man", "Q125449414"),
    ("The Ruse", "Q136654993"),
    ("The Senior", "Q116055747"),
    ("The Shawshank Redemption", "Q172241"),
    ("The Shining", "Q186341"),
    ("The Silent Twins", "Q106597838"),
    ("The Smashing Machine", "Q126012569"),
    ("The Sound", "Q136655029"),
    ("The Space Between", "Q106639036"),
    ("The Sparks Brothers", "Q96412800"),
    ("The SpongeBob Movie: Search for SquarePants", "Q118847776"),
    ("The SpongeBob Movie: Sponge on the Run", "Q61450772"),
    ("The Strangers – Chapter 2", "Q122396990"),
    ("The Substance", "Q113380226"),
    ("The Suicide Squad", "Q57317060"),
    ("The Summer Book", "Q117712753"),
    ("The Super Mario Bros. Movie", "Q108671102"),
    ("The Supremes at Earl's All-You-Can-Eat", "Q114658300"),
    ("The Surrender", "Q134569981"),
    ("The Sword in the Stone", "Q204662"),
    ("The Testament of Ann Lee", "Q131441164"),
    ("The Three Caballeros", "Q736731"),
    ("The Threesome", "Q124630194"),
    ("The Thursday Murder Club", "Q125622191"),
    ("The Tiger's Apprentice", "Q65679603"),
    ("The Tigger Movie", "Q374431"),
    ("The Tomorrow War", "Q67171072"),
    ("The Toxic Avenger", "Q107404778"),
    ("The Trial of the Chicago 7", "Q72397975"),
    ("The Turning", "Q52151357"),
    ("The Twits", "Q122699348"),
    ("The Unbreakable Boy", "Q113380380"),
    ("The Unholy Trinity", "Q123185886"),
    ("The Uninvited", "Q124847978"),
    ("The Way Back", "Q58928205"),
    ("The Wedding Banquet", "Q125660863"),
    ("The Wild Robot", "Q124378349"),
    ("The Wilderness", "Q135916373"),
    ("The Witcher: Sirens of the Deep", "Q123478480"),
    ("The Witches", "Q63630439"),
    ("The Wolf Hour", "Q60833222"),
    ("The Wolverine", "Q197491"),
    ("The Woman in Cabin 10", "Q126119788"),
    ("The Woman in the Window", "Q56085667"),
    ("The Woman in the Yard", "Q125678238"),
    ("The Workout", "Q136655068"),
    ("The World Will Tremble", "Q131820586"),
    ("The Wrecker", "Q122946886"),
    ("The Wrong Paris", "Q131956863"),
    ("Theater Camp", "Q116287765"),
    ("Thinner", "Q1637939"),
    ("This Is the Night", "Q54868320"),
    ("Thor", "Q217020"),
    ("Thor: Love and Thunder", "Q65768604"),
    ("Thor: Ragnarok", "Q22665878"),
    ("Thor: The Dark World", "Q1201853"),
    ("Thunderbolts*", "Q112322474"),
    ("Ticket to Paradise", "Q109511080"),
    ("Tiger", "Q125867263"),
    ("Till", "Q108532414"),
    ("Tim Travers and the Time Traveler's Paradox", "Q134449700"),
    ("Timmy Failure: Mistakes Were Made", "Q55762028"),
    ("Tin Soldier", "Q116735906"),
    ("Tina", "Q105697084"),
    ("Tinker Bell", "Q550571"),
    ("Tinker Bell and the Great Fairy Rescue", "Q1507306"),
    ("Tinker Bell and the Legend of the NeverBeast", "Q18581780"),
    ("Tinker Bell and the Lost Treasure", "Q873598"),
    ("To Die Alone", "Q136654935"),
    ("To Live and Die and Live", "Q116202220"),
    ("Together", "Q131870558"),
    ("Tom & Jerry", "Q65921361"),
    ("Top Gun: Maverick", "Q31202708"),
    ("Toy Story", "Q171048"),
    ("Toy Story 2", "Q187266"),
    ("Toy Story 3", "Q187278"),
    ("Toy Story 4", "Q18517638"),
    ("Train Dreams", "Q126086662"),
    ("Transformers One", "Q116638627"),
    ("Transformers: Rise of the Beasts", "Q107174193"),
    ("Trap House", "Q125388230"),
    ("Traumatika", "Q135488873"),
    ("Treasure Planet", "Q392530"),
    ("Trolls Band Together", "Q117323743"),
    ("Trolls World Tour", "Q29825830"),
    ("Tron: Ares", "Q120237363"),
    ("Trouble Man", "Q135222904"),
    ("Trust", "Q134700472"),
    ("Truth & Treason", "Q133824712"),
    ("Turning Red", "Q104845027"),
    ("Twinless", "Q129978691"),
    ("Twisters", "Q115932556"),
    ("Two Many Chefs", "Q111199436"),
    ("Tár", "Q108649516"),
    ("Uncharted", "Q67171881"),
    ("Under Fire", "Q135336613"),
    ("Under the Boardwalk", "Q113485088"),
    ("Underwater", "Q33105130"),
    ("Unexpected Christmas", "Q136410905"),
    ("Unit 234", "Q112108763"),
    ("Unpregnant", "Q73536578"),
    ("Until Dawn", "Q128602012"),
    ("Up", "Q174811"),
    ("Uppercut", "Q136654989"),
    ("V13", "Q135975533"),
    ("Vacation Friends", "Q102036255"),
    ("Vacation Friends 2", "Q121774936"),
    ("Valiant One", "Q125894001"),
    ("Vampires vs. the Bronx", "Q56086947"),
    ("Venom", "Q23006268"),
    ("Venom: Let There Be Carnage", "Q60498064"),
    ("Venom: The Last Dance", "Q113371337"),
    ("Vicious", "Q124856152"),
    ("Violent Ends", "Q136673339"),
    ("Violent Night", "Q111170246"),
    ("Vivo", "Q96506972"),
    ("Vulcanizadora", "Q126930039"),
    ("Wake Up Dead Man", "Q115931717"),
    ("WALL-E", "Q104905"),
    ("Waltzing with Brando", "Q60738568"),
    ("War of the Worlds", "Q135458343"),
    ("Warfare", "Q125217247"),
    ("Watcher", "Q106554940"),
    ("Watchmen", "Q162182"),
    ("Watchmen: Chapter I", "Q129485956"),
    ("Watchmen: Chapter II", "Q134460009"),
    ("We Strangers", "Q124260477"),
    ("Weapons", "Q126487478"),
    ("Wendy", "Q65084758"),
    ("Werewolf Game", "Q121470799"),
    ("West Side Story", "Q63643994"),
    ("What We Hide", "Q133273812"),
    ("When I'm Ready", "Q132774153"),
    ("Where the Crawdads Sing", "Q106311043"),
    ("White Men Can't Jump", "Q112124858"),
    ("Wicked", "Q24797403"),
    ("Wicked: For Good", "Q114817050"),
    ("Wildcat", "Q133273687"),
    ("Winnie the Pooh", "Q922193"),
    ("Winnie the Pooh: A Very Merry Pooh Year", "Q179411"),
    ("Winnie the Pooh: Seasons of Giving", "Q1440523"),
    ("Winnie the Pooh: Springtime with Roo", "Q1810461"),
    ("Winter Spring Summer or Fall", "Q115929234"),
    ("Wish", "Q113883193"),
    ("Wish Dragon", "Q61740797"),
    ("Wish You Were Here", "Q131543023"),
    ("Witchboard", "Q120828163"),
    ("Without Remorse", "Q65156762"),
    ("Wolf Man", "Q111464581"),
    ("Women Talking", "Q108030443"),
    ("Wonder Woman", "Q20502242"),
    ("Wonder Woman 1984", "Q33408623"),
    ("Woody Woodpecker Goes to Camp", "Q124407153"),
    ("Words of War", "Q116265668"),
    ("World's Best", "Q119938246"),
    ("Wreck-It Ralph", "Q28891"),
    ("X-Men", "Q106182"),
    ("X-Men Origins: Wolverine", "Q217552"),
    ("X-Men: Apocalypse", "Q17042878"),
    ("X-Men: Days of Future Past", "Q4985891"),
    ("X-Men: First Class", "Q223596"),
    ("X-Men: The Last Stand", "Q221168"),
    ("X2", "Q219776"),
    ("Xeno", "Q135108159"),
    ("Y2K", "Q117481301"),
    ("You Should Have Left", "Q60737736"),
    ("You're Cordially Invited", "Q120072944"),
    ("You, Me & Her", "Q132264369"),
    ("Young Woman and the Sea", "Q111982520"),
    ("Zero", "Q133822918"),
    ("Zombies 4: Dawn of the Vampires", "Q125201436"),
    ("Zootopia", "Q15270647"),
    ("Zootopia 2", "Q116764712"),
]

def trakt_auth_refresh():
    TOKENS_PATH = "updates/trakt_tokens.json"; trakt_data = json.load(open(TOKENS_PATH, encoding="utf-8"))
    h={"Content-Type":"application/json","trakt-api-version":"2","trakt-api-key":trakt_data["client_id"],"Authorization":f"Bearer {trakt_data['access_token']}"}; now=time.time(); exp=trakt_data["created_at"]+trakt_data["expires_in"]
    if now<=exp-300: rem=int(exp-now); print(f"🔐 Trakt token valid (~{rem//3600}h {(rem%3600)//60}m)"); return trakt_data,h
    print("⏳ Trakt token expired. Refreshing..."); r=requests.post(trakt_data["baseurl"]+"/oauth/token",json={"grant_type":"refresh_token","refresh_token":trakt_data["refresh_token"],"client_id":trakt_data["client_id"],"client_secret":trakt_data["client_secret"]})
    if r.status_code!=200:
        print("❌ Trakt refresh failed:", r.status_code, (r.text or "")[:300])
        sys.exit(1)
    trakt_data.update(r.json()); trakt_data["created_at"]=time.time(); h["Authorization"]=f"Bearer {trakt_data['access_token']}"; print("🔄 Trakt token refresh completed")
    open(TOKENS_PATH,"w",encoding="utf-8").write(json.dumps(trakt_data,ensure_ascii=False,indent=2))
    return trakt_data,h
    
def gh_sync_qid_issue(missing_titles):
    try:
        tok=(os.getenv("GITHUB_TOKEN") or "").strip()
        repo=(os.getenv("GITHUB_REPOSITORY") or "").strip()
        if not tok or not repo: return
        ISSUE_TITLE="QID missing (auto)"
        h={"Authorization":f"Bearer {tok}","Accept":"application/vnd.github+json","X-GitHub-Api-Version":"2022-11-28"}
        base="https://api.github.com"
        q=f'repo:{repo} is:issue in:title "{ISSUE_TITLE}"'
        r=requests.get(f"{base}/search/issues",params={"q":q},headers=h,timeout=HTTP_TIMEOUT)
        items=(r.json() or {}).get("items") or []
        hit=next((it for it in items if (it.get("title") or "")==ISSUE_TITLE), None)
        num=hit.get("number") if hit else None

        cur_body=""; cur_state=""
        if num:
            rr=requests.get(f"{base}/repos/{repo}/issues/{num}",headers=h,timeout=HTTP_TIMEOUT)
            j=rr.json() or {}
            cur_body=(j.get("body") or "").strip()
            cur_state=(j.get("state") or "").strip()

        if missing_titles:
            body="\n".join(["Missing QIDs:"]+[f"- {t}" for t in missing_titles]).strip()
            payload={"body":body,"state":"open"}
            if num:
                if cur_state!="open" or cur_body!=body:
                    requests.patch(f"{base}/repos/{repo}/issues/{num}",json=payload,headers=h,timeout=HTTP_TIMEOUT)
            else:
                requests.post(f"{base}/repos/{repo}/issues",json={"title":ISSUE_TITLE,**payload,"labels":["automation","qid"]},headers=h,timeout=HTTP_TIMEOUT)
        else:
            if num and cur_state=="open":
                requests.patch(f"{base}/repos/{repo}/issues/{num}",json={"state":"closed"},headers=h,timeout=HTTP_TIMEOUT)
    except Exception as e:
        print("⚠️ GitHub issue sync failed:", type(e).__name__)

def wikipedia_scraping(url:str,selector:Any)->List[WFilm]:
    r=S.get(url,timeout=HTTP_TIMEOUT); doc=html.fromstring(r.content); tbls=doc.xpath("//table[contains(concat(' ', @class, ' '), ' wikitable ')]"); href_filter=None; table_indices=[0]
    if isinstance(selector,dict): href_filter=(selector.get("href_contains") or "").lower(); table_indices=[selector.get("tables")] if isinstance(selector.get("tables"),int) else (selector.get("tables") or [0])
    elif isinstance(selector,list): table_indices=[i for i in selector if isinstance(i,int)]
    elif isinstance(selector,int): table_indices=[selector]
    out=[]; seen=set(); URL_YEAR=(lambda m:int(m.group(1)) if m else TODAY.year)(re.search(r"List_of_American_films_of_(\d{4})",url,re.IGNORECASE)); _txt=lambda e: re.sub(r"\s+"," ",(e.text_content() if e is not None else "")).strip()
    for ti in table_indices:
        if not (0<=ti<len(tbls)): continue
        tbl=tbls[ti]; headers=tbl.xpath(".//tr[th]"); header_row=max(headers,key=lambda tr: len(tr.xpath("./th"))) if headers else None
        header_texts=[re.sub(r"\s+"," ",(th.text_content() or "")).strip().lower() for th in (header_row.xpath("./th") if header_row is not None else [])]
        has_opening=any("opening" in h for h in header_texts); has_title=any(any(x in h for x in ["title","film"]) for h in header_texts); is_calendar=has_opening and has_title
        date_col=(next((i for i,h in enumerate(header_texts) if ("opening" in h)),None) if is_calendar else next((i for i,h in enumerate(header_texts) if any(x in h for x in ["release date","year"])),None)); title_col=next((i for i,h in enumerate(header_texts) if any(x in h for x in ["title","film"])),None); genre_col=(next((i for i,h in enumerate(header_texts) if "genre" in h),None) if href_filter else None)
        if date_col is None or title_col is None: continue
        if is_calendar: day_col=date_col+1; title_col=title_col+1
        else: day_col=date_col
        num_cols=len(header_texts); rowspan_tracker=[0]*num_cols; rowspan_values=[None]*num_cols; current_month=None; current_day=None; last_date=None
        for tr in tbl.xpath('.//tr[td or th[@scope="row"]]'):
            row=[None]*num_cols
            for i in range(num_cols):
                if rowspan_tracker[i]>0: row[i]=rowspan_values[i] if rowspan_values[i] is not None else "ROWSPAN"; rowspan_tracker[i]-=1
            col_idx=0
            for cell in tr.xpath('./th|./td'):
                while col_idx<num_cols and row[col_idx] is not None: col_idx+=1
                if col_idx>=num_cols: break
                colspan=max(1,int(cell.get('colspan') or 1)); rowspan=max(1,int(cell.get('rowspan') or 1))
                for k in range(colspan):
                    if col_idx+k>=num_cols: break
                    if k==0: row[col_idx+k]=cell
                    else: row[col_idx+k]="COLSPAN"
                    if rowspan>1: rowspan_values[col_idx+k]=cell if k==0 else None; rowspan_tracker[col_idx+k]=max(rowspan_tracker[col_idx+k],rowspan-1)
                col_idx+=colspan
            if is_calendar:
                if len(row)>0 and row[0] not in (None,"ROWSPAN","COLSPAN"): mtxt=re.sub(r"[^A-Za-z]","",_txt(row[0])).lower()
                if len(row)>0 and row[0] not in (None,"ROWSPAN","COLSPAN") and mtxt in MONTHS: current_month=mtxt.title()
                if day_col is not None and day_col<len(row) and row[day_col] not in (None,"ROWSPAN","COLSPAN"): mm=re.search(r"\b(\d{1,2})\b",_txt(row[day_col]))
                if day_col is not None and day_col<len(row) and row[day_col] not in (None,"ROWSPAN","COLSPAN") and mm: current_day=int(mm.group(1))
            title=None; page=None; link_state=""
            if title_col<len(row) and row[title_col] not in (None,"ROWSPAN","COLSPAN"):
                tc=row[title_col]; a_list=tc.xpath(".//i//a[starts-with(@href,'/wiki/')][1]") or tc.xpath(".//a[starts-with(@href,'/wiki/')][1]"); a=a_list[0] if len(a_list)>0 else None
                title=(a.text_content().strip() if a is not None else _txt(tc))
                if a is not None:
                    acls=(a.get("class") or "").split()
                    if "new" in acls: page=None; link_state="redlink"
                    else: href=a.get("href",""); page=unquote(href.split("#")[0]).replace("/wiki/","").replace(" ","_"); link_state=""
            if not title:
                for c in row:
                    if c is None or isinstance(c,str): continue
                    a_list=c.xpath(".//a[starts-with(@href,'/wiki/')][1]")
                    if len(a_list)>0:
                        a=a_list[0]; title=(a.text_content() or "").strip(); acls=(a.get("class") or "").split()
                        if "new" in acls: page=None; link_state="redlink"
                        else: href=a.get("href",""); page=unquote(href.split("#")[0]).replace("/wiki/","").replace(" ","_"); link_state=""
                        break
            if title and not page and not link_state: link_state="plain-text"
            if href_filter and genre_col is not None and genre_col<len(row) and row[genre_col] not in (None,"ROWSPAN","COLSPAN"):
                if href_filter not in (_txt(row[genre_col]).lower()): continue
            date_iso=None
            if is_calendar and current_month and current_day and title:
                try: date_iso=datetime.strptime(f"{current_month} {current_day}, {URL_YEAR}","%B %d, %Y").strftime("%Y-%m-%d"); last_date=date_iso
                except: date_iso=None
            elif day_col is not None and day_col<len(row) and row[day_col] not in (None,"ROWSPAN","COLSPAN"):
                raw=re.sub(r"\[\d+\]","",_txt(row[day_col])); low=raw.lower()
                if low in {"tba","tbd"}: date_iso=None
                else:
                    m=DATE_FULL_RE.search(raw)
                    if m: date_iso=datetime.strptime(f"{m.group(1)} {int(m.group(2))}, {int(m.group(3))}","%B %d, %Y").strftime("%Y-%m-%d"); last_date=date_iso
                    else: y=YEAR_RE.search(raw); date_iso=f"{y.group(1)}-01-01" if y else last_date
            else: date_iso=last_date
            if title: title=re.sub(r"[†‡§]+|\[\d+\]|\s+"," ",title).strip()
            if title and title.lower() in {"title","film","tba","tbd"}: title=None
            if title and date_iso:
                try: fdate=datetime.strptime(date_iso,"%Y-%m-%d").date(); full=not date_iso.endswith("-01-01")
                except: fdate=None; full=False
                if fdate and ((full and fdate<=TODAY) or ((not full) and fdate.year<=TODAY.year)):
                    key=f"{title.lower()}#{int(date_iso[:4])}"; 
                    if key not in seen: seen.add(key); out.append(WFilm(title,date_iso,url,page,full,link_state))
    return out

def trakt_sync_list(per_site_films, per_site_pos, per_site_label, per_site_slug,
                    page_to_qid, qid_to_tmdb, title_lower_to_qid, per_site_url, TRAKT_HEADERS):
    norm_key=lambda s: re.sub(r'[^a-z0-9]+','',(s or '').lower())
    ck=lambda s:(s or "").replace(" ","_")

    def _safe_json_response(resp):
        try: js=resp.json()
        except Exception: js={}
        return js or {}

    def _trakt_post_with_retry(session,url,headers,payload,what,max_retries=6,base_sleep=0.8,timeout=(4,10)):
        _TRANSIENT={429,500,502,503,504}; attempt=0; total_wait=0.0
        while True:
            attempt+=1
            try:
                resp=session.post(url,headers=headers,json=payload,timeout=timeout)
            except requests.RequestException:
                if attempt<=max_retries:
                    wait_s=base_sleep*(2**(attempt-1)); time.sleep(wait_s); total_wait+=wait_s; continue
                return 0,{},total_wait,False
            status=resp.status_code
            if status in _TRANSIENT:
                ra=resp.headers.get("Retry-After")
                if ra:
                    try: wait_s=float(ra)
                    except: wait_s=base_sleep*(2**(attempt-1))
                else:
                    wait_s=base_sleep*(2**(attempt-1))
                time.sleep(wait_s); total_wait+=wait_s
                if attempt<=max_retries: continue
                js=_safe_json_response(resp); return status,js,total_wait,(status==429)
            js=_safe_json_response(resp); return status,js,total_wait,(status==429)

    def _fetch_trakt_items(base,headers):
        cur=[]; pg=1
        while True:
            r=S.get(base+"/items/movies",headers=headers,params={"extended":"full","page":pg,"limit":100},timeout=(4,10))
            js=_safe_json_response(r); js=js if isinstance(js,list) else []; cur+=js
            pc=int(r.headers.get("X-Pagination-Page-Count","1") or "1")
            if pg>=pc or not js: break
            pg+=1
        return cur

    def _pick_one(arr):
        if not arr: return None
        for v in arr:
            sv=str(v).strip()
            if sv: return sv
        return None

    def _tmdb_from_qid(qid):
        if not qid: return None
        return _pick_one(qid_to_tmdb.get(qid,[]))

    qids_manual_map={}
    for t,q in QIDS:
        k=norm_key(t)
        if (q or "").strip():
            qids_manual_map[k]=q.strip()

    title_lower_to_qid_norm={ norm_key(k):v for k,v in title_lower_to_qid.items() }

    def tmdb_id_for_film(f):
        nk=norm_key(getattr(f,'title','') or '')
        qid_manual=qids_manual_map.get(nk)
        if qid_manual:
            tm=_tmdb_from_qid(qid_manual)
            if tm: return tm
        if getattr(f,'page',None):
            qid=page_to_qid.get(ck(f.page)); tm=_tmdb_from_qid(qid)
            if tm: return tm
        qid_auto=title_lower_to_qid_norm.get(nk); tm=_tmdb_from_qid(qid_auto)
        if tm: return tm
        return None

    idxs=list(range(len(per_site_films)))
    groups_by_slug={}
    for i in idxs: groups_by_slug.setdefault(per_site_slug[i], []).append(i)

    label2first_idx={}
    for i in idxs: label2first_idx.setdefault(per_site_label[i], i)

    canonical_label={
        "ink-magic-a-disney-animation-journey": "🪄 Ink & Magic – A Disney Animation Journey",
        "warner-bros-toons-films-tv": "🐰 Warner Bros. Toons — Films & TV"
    }

    label2idx_display={}
    for slug, idx_list in groups_by_slug.items():
        label_out=canonical_label.get(slug, per_site_label[idx_list[0]])
        label2idx_display[label_out]=min(idx_list)

    def variant_tag(label:str):
        L=label.lower()
        if "wdas" in L: return "WDAS"
        if "dtva" in L: return "DTVA"
        if "dts"  in L: return "DTS"
        if " d+ " in f" {label} ": return "D+"
        if "pixar" in L: return "Pixar"
        if "pictures animation" in L: return "WBPA"
        if "animation" in L: return "WBA"
        return label

    all_missing_rows=[]; all_trakt_missing_rows=[]; titles_for_qids_global=set()
    collisions_agg=defaultdict(lambda: {"titles": set(), "sites": set(), "site_idxs": set(), "pos_by_site": {}})
    missing_agg=defaultdict(lambda: {"labels": set(), "site_idxs": set(), "pos_by_site": {}, "min_pos": 10**9})
    trakt_missing_agg=defaultdict(lambda: {"labels": set(), "site_idxs": set(), "pos_by_site": {}, "min_pos": 10**9})

    tmdb_global_map=defaultdict(set)
    for i in idxs:
        for f in per_site_films.get(i,[]):
            tm=tmdb_id_for_film(f)
            if tm: tmdb_global_map[tm].add(f.title)

    tmdb_in_collision={tm for tm,tits in tmdb_global_map.items() if len({t.strip() for t in tits})>=2}

    for slug, idx_list in groups_by_slug.items():
        uniq=[]; seen=set(); pos_union={}
        for i in idx_list:
            films=per_site_films.get(i,[])
            for f in films:
                k=(f.title.strip().lower(), f.date_iso[:4] if f.date_iso else None)
                if k in seen: continue
                seen.add(k); pos_union[k]=len(uniq); uniq.append(f)

        title_to_year={}
        for f in uniq:
            y=(f.date_iso[:4] if f and f.date_iso else None); title_to_year.setdefault(f.title.strip().lower(), y)

        tm_to_titles_local=defaultdict(set); tmdb2title={}
        for f in uniq:
            tm=tmdb_id_for_film(f)
            if tm:
                tm_to_titles_local[tm].add(f.title)
                tmdb2title.setdefault(tm,f.title)

        blocked_tm={ tm for tm in tm_to_titles_local if tm in tmdb_in_collision }

        for tm,titles in tm_to_titles_local.items():
            if tm in blocked_tm:
                for i in idx_list:
                    lbl=per_site_label[i]
                    for t in titles:
                        key=(t.strip().lower(), title_to_year.get(t.strip().lower()))
                        if key in per_site_pos.get(i,{}):
                            collisions_agg[tm]["titles"].add(t); collisions_agg[tm]["sites"].add(lbl); collisions_agg[tm]["site_idxs"].add(i)
                            pos_i=per_site_pos[i][key]; prev=collisions_agg[tm]["pos_by_site"].get(i, 10**9)
                            if pos_i<prev: collisions_agg[tm]["pos_by_site"][i]=pos_i

        site_tmdb_target={ tm for tm in tm_to_titles_local if tm not in blocked_tm }

        base=None; r=S.get(f"https://api.trakt.tv/users/dtvabrand/lists/{slug}",headers=TRAKT_HEADERS,timeout=(4,10)); 
        if r.status_code<400: base=r.request.url
        if base is None:
            label0=per_site_label[idx_list[0]]; name_for_trakt=re.sub(r'^[^\w]+','',label0).strip()
            urls_for_slug=sorted({ per_site_url.get(i) for i in idx_list if per_site_url.get(i) }); desc="\n".join(urls_for_slug)
            _,created,_,_=_trakt_post_with_retry(S,"https://api.trakt.tv/users/dtvabrand/lists",TRAKT_HEADERS,{"name":name_for_trakt,"description":desc,"privacy":"public","display_numbers":False,"allow_comments":False,"sort_by":"released","sort_how":"asc"},"create list")
            got=((created.get("ids") or {}).get("slug") if isinstance(created,dict) else None); base=f"https://api.trakt.tv/users/dtvabrand/lists/{got or slug}"

        before=_fetch_trakt_items(base,TRAKT_HEADERS)
        existing_tmdb=set(); extraneous_trakt_ids=[]
        for it in before:
            mv=(it.get("movie") or {}); ids=(mv.get("ids") or {})
            tm=ids.get("tmdb"); tid=ids.get("trakt")
            if tm is None:
                if tid is not None: extraneous_trakt_ids.append(int(tid))
            else:
                existing_tmdb.add(str(tm))

        wrong_ids=sorted({tm for tm in existing_tmdb if tm not in site_tmdb_target and tm and tm.isdigit()}, key=int)
        to_rm=[{"ids":{"tmdb":int(tm)}} for tm in wrong_ids]
        to_rm_trakt=[{"ids":{"trakt":tid}} for tid in extraneous_trakt_ids]
        to_add=[{"ids":{"tmdb":int(tm)}} for tm in site_tmdb_target if tm not in existing_tmdb and tm and tm.isdigit()]

        if to_rm_trakt:
            for j in range(0,len(to_rm_trakt),100):
                _trakt_post_with_retry(S,base+"/items/remove",TRAKT_HEADERS,{"movies":to_rm_trakt[j:j+100]},"remove items (non-TMDb)")
        if to_rm:
            for j in range(0,len(to_rm),100):
                _trakt_post_with_retry(S,base+"/items/remove",TRAKT_HEADERS,{"movies":to_rm[j:j+100]},"remove items")
        if to_add:
            for j in range(0,len(to_add),100):
                _trakt_post_with_retry(S,base+"/items",TRAKT_HEADERS,{"movies":to_add[j:j+100]},"add items")

        after=_fetch_trakt_items(base,TRAKT_HEADERS)
        after_tmdb={str((it.get("movie") or {}).get("ids",{}).get("tmdb")) for it in after if (it.get("movie") or {}).get("ids",{}).get("tmdb") is not None}

        missing_no_tmdb=[]
        for f in uniq:
            tm=tmdb_id_for_film(f)
            if not tm:
                key=(f.title.strip().lower(), (f.date_iso or "")[:4]); pos=pos_union.get(key,10**9)
                missing_no_tmdb.append((pos,f.title))

        want_not_in_after = sorted({tm for tm in site_tmdb_target if tm not in after_tmdb},
                                   key=lambda x: int(x) if str(x).isdigit() else 10**12)

        trakt_missing=[]
        for tm in want_not_in_after:
            rep_title = tmdb2title.get(tm, f"TMDb:{tm}")
            key=(rep_title.strip().lower(), (next((ff.date_iso for ff in uniq if ff.title==rep_title), "")[:4]))
            pos=pos_union.get(key,10**9); trakt_missing.append((pos,rep_title))

        total_films=len(uniq); in_list=len(after_tmdb)

        def breakdown_counts():
            per_tag={}; title_to_tag={}
            for i in idx_list:
                tag=variant_tag(per_site_label[i])
                for f in per_site_films.get(i,[]): title_to_tag.setdefault(f.title, tag)
            def add(tag):
                if tag: per_tag[tag]=per_tag.get(tag,0)+1
            for _,t in missing_no_tmdb: add(title_to_tag.get(t,""))
            for _,t in trakt_missing:   add(title_to_tag.get(t,""))
            parts=[f"{n} {tg}" for tg,n in per_tag.items() if n>0]
            return f" ({', '.join(parts)})" if parts else ""

        label_out=canonical_label.get(slug, per_site_label[idx_list[0]])
        miss_total=len(missing_no_tmdb)+len(trakt_missing)
        is_multi=len(idx_list)>1 and slug in ("ink-magic-a-disney-animation-journey","warner-bros-toons-films-tv")
        if is_multi:
            print(f"{label_out} – {total_films} film  |  🧩 Trakt list: {in_list}  |  ⚠️ Missing: {miss_total}{breakdown_counts()}")
        else:
            print(f"{label_out} – {total_films} film  |  🧩 Trakt list: {in_list}  |  ⚠️ Missing: {miss_total}")

        for (_pos,title) in missing_no_tmdb:
            dat=missing_agg[title]; dat["min_pos"]=min(dat["min_pos"], _pos)
            for i in idx_list:
                key=(title.strip().lower(), title_to_year.get(title.strip().lower()))
                if key in per_site_pos.get(i,{}):
                    dat["labels"].add(label_out); dat["site_idxs"].add(i)
                    pos_i=per_site_pos[i][key]; prev=dat["pos_by_site"].get(i, 10**9)
                    if pos_i<prev: dat["pos_by_site"][i]=pos_i

        for (_pos,title) in trakt_missing:
            dat=trakt_missing_agg[title]; dat["min_pos"]=min(dat["min_pos"], _pos)
            for i in idx_list:
                key=(title.strip().lower(), title_to_year.get(title.strip().lower()))
                if key in per_site_pos.get(i,{}):
                    dat["labels"].add(label_out); dat["site_idxs"].add(i)
                    pos_i=per_site_pos[i][key]; prev=dat["pos_by_site"].get(i, 10**9)
                    if pos_i<prev: dat["pos_by_site"][i]=pos_i

        for _,t in missing_no_tmdb: titles_for_qids_global.add(t)
        for _,t in trakt_missing: titles_for_qids_global.add(t)

    for dat in collisions_agg.values():
        for tt in dat["titles"]: titles_for_qids_global.add(tt)

    print("")
    if missing_agg:
        def _key_missing(item):
            title, dat=item; msi=min(dat["site_idxs"]) if dat["site_idxs"] else 10**9
            pos=dat["pos_by_site"].get(msi, dat["min_pos"]); return (msi, pos, title.lower())
        print(f"⚠️ TMDb Movie ID missing ({len(missing_agg)})")
        for title, dat in sorted(missing_agg.items(), key=_key_missing):
            labels_sorted=sorted(dat["labels"], key=lambda lbl: label2idx_display.get(lbl, 10**9))
            print(f"• {title} — {', '.join(labels_sorted)}")
        print("")

    if collisions_agg:
        def _key_collision(item):
            _tm, dat=item; msi=min(dat["site_idxs"]) if dat["site_idxs"] else 10**9
            pos=dat["pos_by_site"].get(msi, 10**9)
            titles_joined=", ".join(sorted(dat["titles"], key=str.lower))
            return (msi, pos, titles_joined.lower())
        print(f"⚠️ TMDb Movie ID collisions ({len(collisions_agg)})")
        for _tm, dat in sorted(collisions_agg.items(), key=_key_collision):
            titles_joined=", ".join(sorted(dat["titles"], key=str.lower))
            sites_sorted=sorted(dat["sites"], key=lambda lbl: label2first_idx.get(lbl, 10**9))
            print(f"• {titles_joined} — {', '.join(sites_sorted)}")
        print("")

    if trakt_missing_agg:
        def _key_trakt(item):
            title, dat=item; msi=min(dat["site_idxs"]) if dat["site_idxs"] else 10**9
            pos=dat["pos_by_site"].get(msi, dat["min_pos"]); return (msi, pos, title.lower())
        print(f"⚠️ Trakt film missing ({len(trakt_missing_agg)})")
        for title, dat in sorted(trakt_missing_agg.items(), key=_key_trakt):
            labels_sorted=sorted(dat["labels"], key=lambda lbl: label2idx_display.get(lbl, 10**9))
            print(f"• {title} — {', '.join(labels_sorted)}")

    def _title_order_info():
        info={}
        def upd(t, site_idxs, pos_by_site, min_pos):
            if site_idxs: msi=min(site_idxs); pos=pos_by_site.get(msi, min_pos); cur=(msi, pos)
            else: cur=(10**9, min_pos)
            prev=info.get(t, (10**9,10**9))
            if cur<prev: info[t]=cur
        for title, dat in missing_agg.items(): upd(title, dat["site_idxs"], dat["pos_by_site"], dat["min_pos"])
        for title, dat in trakt_missing_agg.items(): upd(title, dat["site_idxs"], dat["pos_by_site"], dat["min_pos"])
        for _tm, dat in collisions_agg.items():
            for t in dat["titles"]: upd(t, dat["site_idxs"], dat["pos_by_site"], 10**9)
        return info

    order_info=_title_order_info()

    manual_exact={t:q for (t,q) in QIDS if (q or "").strip()}
    existing_by_norm={ norm_key(t): q for (t,q) in QIDS if (q or "").strip() }

    def q_for_title(t):
        nk=norm_key(t)
        return (manual_exact.get(t,"") or existing_by_norm.get(nk,"") or title_lower_to_qid_norm.get(nk,""))

    scraped_title_norms=set()
    for i in per_site_films:
        for f in per_site_films.get(i,[]) or []:
            scraped_title_norms.add(norm_key(getattr(f,'title','') or ''))

    preserved_manual=[(t,q) for (t,q) in QIDS
                      if (q or "").strip() and norm_key(t) in scraped_title_norms]

    preserved_titles=set(t for (t,_) in preserved_manual)
    wanted_titles=set(titles_for_qids_global) | preserved_titles

    def sort_key_title(t):
        return (order_info.get(t,(10**9,10**9))[0], order_info.get(t,(10**9,10**9))[1], t.lower())

    sorted_titles=sorted(wanted_titles, key=sort_key_title)

    out=[]
    seen_norm=set()
    for t in sorted_titles:
        nk=norm_key(t)
        if nk in seen_norm: continue
        q_manual=next((q for (tt,q) in preserved_manual if tt==t and (q or "").strip()), "")
        q_auto=q_for_title(t)
        q = q_manual or q_auto or ""
        out.append((t,q))
        seen_norm.add(nk)

    missing_titles=[t for (t,q) in out if not (q or "").strip()]
    if missing_titles: print("📝 Missing QIDs:", ", ".join(f'"{t}"' for t in missing_titles))
    gh_sync_qid_issue(missing_titles)
    p=os.path.realpath(__file__); s=open(p,encoding="utf-8").read()
    prev_titles={t for (t,_) in QIDS}; added_titles=[t for (t,_) in out if t not in prev_titles]
    if added_titles:
        uniq=list(dict.fromkeys(added_titles))
        log_msg="📝 QID for " + ", ".join(f'"{t}"' for t in uniq); print(log_msg)
    def _dump_QIDS():
        esc=lambda x: json.dumps(str(x),ensure_ascii=False)
        return "QIDS = [\n" + "\n".join(f"    ({esc(m)}, {esc(q)})," for (m,q) in out) + "\n]\n"
    pat = re.compile(r"(?ms)^QIDS\s*=\s*\[.*?\]\s*(?:\r?\n)*")
    block=_dump_QIDS()
    s_new = pat.sub(block + "\n", s) if pat.search(s) else re.sub(r"(?ms)(^SITES\s*=\s*\[.*?\]\s*)", r"\1\n"+block+"\n", s)
    if s_new!=s:
        fd,tmp=tempfile.mkstemp(dir=os.path.dirname(p) or ".",suffix=".py")
        os.fdopen(fd,"w",encoding="utf-8").write(s_new)
        os.replace(tmp,p)

def main():
    _,TRAKT_HEADERS=trakt_auth_refresh()
    variant_label=lambda base,variant: ("🏰 Ink & Magic – WDAS" if variant=="WDAS" else ("🪄 Ink & Magic – DTVA" if variant=="DTVA" else ("🔮 Ink & Magic – DTS" if variant=="DTS" else ("🌠 Ink & Magic – D+" if variant=="D+" else ("💡 Ink & Magic – Pixar" if variant=="Pixar" else ("🐰 Warner Bros. Pictures Animation" if variant=="WBPA" else ("🥕 Warner Bros. Animation" if variant=="WBA" else base)))))))
    per_site_films={}; per_site_pos={}; per_site_label={}; per_site_slug={}; per_site_url={}
    with ThreadPoolExecutor(max_workers=SITE_WORKERS) as ex:
        futs={ex.submit(wikipedia_scraping,u,sel):(i,name,variant,slug) for i,(u,name,variant,slug,sel) in enumerate(SITES)}
        for fu in as_completed(futs):
            i,name,variant,slug=futs[fu]
            try: films=fu.result()
            except Exception: films=[]
            label=variant_label(name,variant); seen=set(); uniq=[]; pos={}
            for f in films:
                key=(f.title.strip().lower(), f.date_iso[:4] if f.date_iso else None)
                if key in seen: continue
                seen.add(key); pos[key]=len(uniq); uniq.append(f)
            per_site_films[i]=uniq; per_site_pos[i]=pos; per_site_label[i]=label; per_site_slug[i]=slug; per_site_url[i]=SITES[i][0]

    all_films=[]
    for i in range(len(SITES)): all_films.extend(per_site_films.get(i,[]))

    ck=lambda s:(s or "").replace(" ","_")

    pages=list(dict.fromkeys([f.page for f in all_films if f.page])); page_to_qid={}
    with ThreadPoolExecutor(max_workers=WD_WORKERS) as ex:
        futs=[ex.submit(S.get,"https://en.wikipedia.org/w/api.php",
                        params={"action":"query","prop":"pageprops","ppprop":"wikibase_item","format":"json",
                                "titles":"|".join(pages[i:i+50]),"redirects":1},
                        timeout=HTTP_TIMEOUT) for i in range(0,len(pages),50)]
        for fu in as_completed(futs):
            try:
                q=fu.result().json().get("query") or {}; canon={}
                for pg in (q.get("pages") or {}).values():
                    t=(pg.get("title") or "").strip(); qid=((pg.get("pageprops") or {}).get("wikibase_item"))
                    if t: canon[t]=qid; page_to_qid[ck(t)]=qid
                for rdir in (q.get("normalized") or [])+(q.get("redirects") or []):
                    f=(rdir.get("from") or "").strip(); t=(rdir.get("to") or "").strip()
                    if f and t:
                        qid=canon.get(t) or page_to_qid.get(ck(t))
                        if qid is not None: page_to_qid[ck(f)]=qid
            except: pass

    qids_from_pages=[page_to_qid.get(ck(f.page)) for f in all_films if f.page and page_to_qid.get(ck(f.page))]
    qids_from_qids=[(q or "").strip() for (_t,q) in QIDS if (q or "").strip()]
    qids=sorted({*qids_from_pages,*qids_from_qids})

    qid_to_tmdb={}
    with ThreadPoolExecutor(max_workers=WD_WORKERS) as ex:
        futs=[ex.submit(S.get,"https://www.wikidata.org/w/api.php",
                        params={"action":"wbgetentities","format":"json","formatversion":2,
                                "ids":"|".join(qids[i:i+50]),"props":"claims"},
                        timeout=HTTP_TIMEOUT) for i in range(0,len(qids),50)]
        for fu in as_completed(futs):
            try:
                for e in (fu.result().json().get("entities") or {}).values():
                    cl=(e.get("claims") or {})
                    vals=lambda p:[(c.get("mainsnak",{}).get("datavalue") or {}).get("value") for c in cl.get(p,[]) if c.get("mainsnak",{}).get("datavalue")]
                    tmdb=[str(v) for v in vals("P4947") if v is not None]
                    qid_to_tmdb[e.get("id") or ""]=tmdb
            except: pass

    title_lower_to_qid={}
    for i in range(len(SITES)):
        for f in per_site_films.get(i,[]):
            if f.page:
                qid=page_to_qid.get(ck(f.page))
                if qid: title_lower_to_qid.setdefault(f.title.strip().lower(),qid)
    for t,q in QIDS:
        qq=(q or "").strip()
        if qq:
            tl=t.strip().lower()
            if tl and tl not in title_lower_to_qid:
                title_lower_to_qid[tl]=qq

    trakt_sync_list(per_site_films, per_site_pos, per_site_label, per_site_slug,
                    page_to_qid, qid_to_tmdb, title_lower_to_qid, per_site_url, TRAKT_HEADERS)

if __name__ == "__main__":
    main()
