from .Models.Yeast import Yeast

class YeastPageParser :
    def parse_page(self, url : str) -> Yeast :
        headersList = {
            "Accept": "*/*",
            "User-Agent": "Thunder Client (https://www.thunderclient.io)"
        }

        response = requests.get(page_link, headers=headersList)
        print("Reading page {}".format(page_link))
        if response.status_code >= 400:
            print("Could not read page, caught error {}".format(response.status_code))
            print("Response error message is {}".format(response.text))
            return ''

        contents = response.content
        soup = BeautifulSoup(contents, 'html.parser')

        for elem in soup.find_all('div', href=False, attrs={'class': 'kl-title-aff'}):
            yeast = Yeast()
            yeast_block = elem.find('a', href=True)
            yeast.name = yeast_block.find(
                'h3', attrs={'itemprop': 'name'}).contents[0]
            yeast.link = yeast_block.attrs['href']
            yeasts.append(yeast)

        next_page = soup.find(
            'a', attrs={'class': 'page-link'}, href=True, string='Suivant')
        if next_page != None:
            return next_page.attrs['href']


    def remove_latin_escaped(self, string: str):
        return string.replace(u'\xa0', u' ')


    def parse_yeast(self, yeast: Yeast):
        headersList = {
            "Accept": "*/*",
            "User-Agent": "Thunder Client (https://www.thunderclient.io)"
        }
        response = requests.get(yeast.link, headers=headersList)
        print("Reading yeast data for {}".format(yeast.name))
        if response.status_code >= 400:
            print("Could not read page, caught error {}".format(response.status_code))
            print("Response error message is {}".format(response.text))
            return yeast

        contents = response.content
        soup = BeautifulSoup(contents, 'html.parser')
        price_div = soup.find('div', attrs={'class': 'current-price'})
        try:
            price_span = price_div.find('span', attrs={'itemprop': 'price'})
            yeast.price = price_span.attrs['content']
        except AttributeError or Exception:
            yeast.price = 'No data'

        name_field = soup.find('h1', attrs={'itemprop': 'name'})
        yeast.name = name_field.next
        description_section = soup.find('section', attrs={'class': 'kl-bg-grey'})
        if description_section is None:
            raise NotAYeastError(yeast.link)

        all_p_blocks = description_section.find_all('p')
        # Shitty code because all yeasts are not all displayed the same way
        try:
            description_block = all_p_blocks[1] if (
                len(all_p_blocks) > 1) else all_p_blocks[0]
            yeast.description = str(
                description_block.next) if description_block.next is not None else ''
        except Exception:
            yeast.description = "Error reading description"

        floculation = description_section.find('strong', string=re.compile("Floculation.*"))
        attenuation = description_section.find('strong', string=re.compile("Atténuation.*"))
        temperature = description_section.find('strong', string=re.compile("Gamme de Température.*"))
        abv = description_section.find('strong', string=re.compile("Tolerance d'alcool.*"))

        try:
            yeast.floculation = floculation.nextSibling
        except (AttributeError):
            yeast.floculation = "NA"
        try:
            yeast.attenuation_range[0] = attenuation.nextSibling
        except (AttributeError):
            yeast.attenuation_range[0] = "NA"
            yeast.attenuation_range[1] = "NA"

        try:
            yeast.temp_range[0] = temperature.nextSibling
        except (AttributeError):
            yeast.temp_range[0] = "NA"
            yeast.temp_range[1] = "NA"

        try:
            yeast.abv_tol = abv.nextSibling
        except (AttributeError):
            yeast.abv_tol = "NA"

        return yeast


    def removed_mislabled_yeasts(self, yeasts_collection : list, mislabled_yeasts : list):
        out_yeasts = []
        for yeast in yeasts_collection:
            mislabled_found = False
            for mislabled in mislabled_yeasts:
                if yeast.name == mislabled.name:
                    mislabled_found = True
                    break
            if not mislabled_found:
                out_yeasts.append(yeast)
        return out_yeasts

    def spread_load_accross_threads(self, max_threads : int, yeasts : list[Yeast] ) -> list[list[Yeast]] :
        remainder = len(yeasts) % max_threads
        yeasts_per_thread = len(yeasts) // max_threads

        # Collection of yeasts for each thread
        threads_yeast_table : list[list[Yeast]] = []

        current_thread_payload : list[Yeast] = []
        remaining_per_thread = yeasts_per_thread
        i = 0
        while i < len(yeasts) :
            if remaining_per_thread != 0:
                current_thread_payload.append(yeasts[i])
                i += 1
                remaining_per_thread -= 1

                if remaining_per_thread == 0 :
                    if remainder > 0:
                        current_thread_payload.append(yeasts[i])
                        i += 1
                        remainder -= 1
                    threads_yeast_table.append(current_thread_payload.copy())
                    current_thread_payload.clear()
                    remaining_per_thread = yeasts_per_thread

        # Append the remaining data, last thread yeast collection
        if len(current_thread_payload) != 0 :
            threads_yeast_table.append(current_thread_payload.copy())

        return threads_yeast_table


    def parse_yeasts_threaded(self, yeasts : list[Yeast], mislabled_yeasts : list[Yeast], error_yeasts : list[Yeast]) :
        mislabled_yeasts = []
        errors_yeasts : list[Yeast] = []
        for yeast in yeasts :
            try:
                yeast = parse_yeast(yeast)
                yeast.format_data()
            except NotAYeastError:
                mislabled_yeasts.append(yeast)
            except Exception :
                errors_yeasts.append(yeast)
