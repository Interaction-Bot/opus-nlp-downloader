from bs4 import BeautifulSoup

import aiofiles.os
import tempfile
import aiofiles
import aiohttp
import asyncio
import zipfile
import shutil
import click


class Opus:
    def __init__(self):
        self.output = {}
        self.src = None
        self.tgt = None

    def get(self, src: str, tgt: str, max_corpus: int = 10):
        print('get...')
        return asyncio.run(self.async_get(src, tgt, max_corpus))

    def download(self, path:str, max_sentences:int = 20_000_000):
        print('download...')
        return asyncio.run(self.async_download(path, max_sentences))

    async def async_get(self, src: str, tgt: str, max_corpus: int = 10):
        async with aiohttp.ClientSession() as session:
            async with session.post('https://opus.nlpl.eu/index.php', data={'src': src, 'trg': tgt, 'minsize': 'all'}) as resp:
                rep = await resp.text()

                soup = BeautifulSoup(rep, 'html.parser')
                html = soup.find("div", {"class": "counts"}).table.find_all('tr')

                for i, el in enumerate(html):
                    if i != 0 and i != len(html)-1:
                        if i > max_corpus+1:
                            break

                        corpus = el.td.b.get_text()
                        
                        async with session.get(f'https://opus.nlpl.eu/opusapi/?corpus={corpus}&source={src}&target={tgt}&preprocessing=moses&version=latest') as resp:
                            rep = await resp.json()

                            if rep['corpora'] != []:
                                self.output[corpus] = {'links': rep['corpora'][0]['url'], 'sentences': rep['corpora'][0]['alignment_pairs'] if \
                                    rep['corpora'][0]['alignment_pairs'] != '' else 0}

            self.src = src 
            self.tgt = tgt

            return self.output

    async def async_download(self, path:str, max_sentences:int = 100_000):
        if self.output == {}:
            raise ValueError("No corpus aviable for this pair.")

        output = {}

        dirpath = tempfile.mkdtemp()

        total_sentences = 0
        for i, v in enumerate(self.output):
            if total_sentences > max_sentences:
                break
            
            dirpath = tempfile.mkdtemp()

            async with aiohttp.ClientSession() as session:
                async with session.get(self.output[v]['links'], timeout=None) as resp:
                    if resp.status == 200:
                        print('download ', v)
                        while True:
                            chunk = await resp.content.read(16144)
                            if not chunk:
                                break

                            f = await aiofiles.open(f'{dirpath}/{v}.zip', mode='ab')
                            await f.write(chunk)
                            
                            await f.close()
                        print('completed')
            total_sentences += self.output[v]['sentences']

            for f in await aiofiles.os.listdir(dirpath):
                with zipfile.ZipFile(f'{dirpath}/'+f,"r") as zip_ref:
                    zip_ref.extractall(dirpath)
                

                shutil.move(f"{dirpath}/{f.split('.')[0]}.{self.tgt}-{self.src}.{self.src}", f"{path}{f.split('.')[0]}-{self.src}.txt")
                shutil.move(f"{dirpath}/{f.split('.')[0]}.{self.tgt}-{self.src}.{self.tgt}", f"{path}{f.split('.')[0]}-{self.tgt}.txt")

                output[f.split('.')[0]] = {
                    'src': f"{f.split('.')[0]}-{self.src}.txt",
                    'tgt': f"{f.split('.')[0]}-{self.tgt}.txt",
                }

        return output


@click.group()
@click.pass_context
def cli(ctx):
    ctx.obj = Opus()

@click.command()
@click.argument('src')
@click.argument('tgt')
@click.option('--max_corpus', type=int, default=10)
@click.pass_obj
def get(ctx, src: str, tgt: str, max_corpus: int = 10):
    print(ctx.get(src, tgt, max_corpus))

@click.command()
@click.argument('src')
@click.argument('tgt')
@click.argument('path')
@click.option('--max_corpus', type=int, default=10)
@click.option('--max_sentences', type=int, default=20_000_000)
@click.pass_obj
def download(ctx, src: str, tgt: str, path: str, max_sentences: int = 20_000_000, max_corpus: int = 10):
    ctx.get(src, tgt, max_corpus)
    print(ctx.download(path, max_sentences))

if __name__ == "__main__":
    cli.add_command(get)
    cli.add_command(download)
    cli()