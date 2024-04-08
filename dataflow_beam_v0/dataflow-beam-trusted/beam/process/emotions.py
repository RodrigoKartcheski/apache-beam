import re
import json
import apache_beam as beam


class ExtractTagsTransform(beam.DoFn):
#    """
#     Descrição da função: Esta função lê a tabela SELECT * FROM `focus-mechanic-321819.beegdata_engineer.lista_emojis`
#     e trata as colunas predefinidas no pipeline do arquivos onde contém class="react-emoji  alterando a string da coluna de forma que o resultado seja um emoji.
#     """
    def __init__(self, column_names, dict_emotions):
        self.column_names = column_names
        self.dict_emotions = dict_emotions

    @staticmethod
    def extract_emojis(text):
        pattern = r'class="react-emoji".*?\/assets\/upload\/react\/(.*?)\.png'
        emojis = re.findall(pattern, text)
        return emojis

    def process(self, element):
        for column_name in self.column_names:
            if column_name in element:
                coluna = element[column_name]
                emojis = self.extract_emojis(coluna)
                for i, emoji in enumerate(emojis):
                    if emoji in self.dict_emotions:
                        emojis[i] = self.dict_emotions[emoji]
                element[column_name] = " ".join(emojis)

        yield element


column_names = ['text']

dict_emotions = [
    {'NomeReact': '703a6d4d4fb3d25225fb205ddc0b78cf', 'Unicode': 'U+2600'}, {'NomeReact': 'd5a3ebe3e6de2e02cce0d08b366e3a03', 'Unicode': 'U+2602'}, {'NomeReact': '293f8b4395b4b8f84d960178b35aca91', 'Unicode': 'U+2615'}, {'NomeReact': '14c39ce032a184d6aa72bf10a3fed91a', 'Unicode': 'U+261D'}, {'NomeReact': '31f52c42f826acfce8ec6efc2694c554', 'Unicode': 'U+2622'}, {'NomeReact': 'e1667b390db7e0a41eb8fcc9ce2c7245', 'Unicode': 'U+262E'}, {'NomeReact': '664c1c1a9f24d93933fd86880cd5b387', 'Unicode': 'U+267B'}, {'NomeReact': 'f3d04819231b4273ebb6502f4de94d97', 'Unicode': 'U+2693'}, {'NomeReact': '183a143138f0fa86717fcc7680667b28', 'Unicode': 'U+26A0'}, {'NomeReact': '5797fe87bf19006df7fedcc849864378', 'Unicode': 'U+26BD'}, {'NomeReact': '21a722f5e269c3a95cd2a28e587ef88a', 'Unicode': 'U+26BE'}, {'NomeReact': '8afc39166c35f1821b453ca6493b1044', 'Unicode': 'U+26C8'}, {'NomeReact': 'b8a6458bc6715cd8b4fdf41999c3df7a', 'Unicode': 'U+26EA'}, {'NomeReact': 'ba5e4c38d4c6fa85fa31161203d72b93', 'Unicode': 'U+2705'}, {'NomeReact': '34af9f01382a059857ef33f72ec711ab', 'Unicode': 'U+2708'}, {'NomeReact': '7b1f0ec9817fef85356985c59c1b6c16', 'Unicode': 'U+270B'}, {'NomeReact': '3219b0232ce4076d64fcfecec7d947ea', 'Unicode': 'U+270C'}, {'NomeReact': 'f9dbbfec386868614b0d7df2744eee4a', 'Unicode': 'U+2744'}, {'NomeReact': 'cd84f6d2bf173bef80feced394b07e77', 'Unicode': 'U+274E'}, {'NomeReact': '20ab2ac74ea481bad33ae710b927e18c', 'Unicode': 'U+2764'}, {'NomeReact': 'e3507c2fb70f2f3f6249f554ea1a63d5', 'Unicode': 'U+2B50'}, {'NomeReact': '4c1c693bd89a7027316d5384b45fbf71', 'Unicode': 'U+1F193'}, {'NomeReact': 'f5ab64fe82f87ec5dd225d205f806716', 'Unicode': 'U+1F198'}, {'NomeReact': 'f28b5e2e812669caa5ee759e6c86c5a5', 'Unicode': 'U+1F309'}, {'NomeReact': 'f759a05632f628b9ca0f3d708cf446c4', 'Unicode': 'U+1F30A'}, {'NomeReact': '89434f131879bd49e011a82d60432ff4', 'Unicode': 'U+1F319'}, {'NomeReact': 'c0729754fc1baf2fa6bfa1f330e15193', 'Unicode': 'U+1F31E'}, {'NomeReact': 'd04dfa40eeb26d56753ccbd02e6ee9fd', 'Unicode': 'U+1F325'}, {'NomeReact': 'ec0ddaf6f716a8914ba213077978060d', 'Unicode': 'U+1F32D'}, {'NomeReact': '3d98d7c4082a3bbff2df5beec6faa09b', 'Unicode': 'U+1F339'}, {'NomeReact': 'c0eb324a75d2f67ef51a36c8bc61540e', 'Unicode': 'U+1F340'}, {'NomeReact': '1538ebe564125897ff81e8eade9f443b', 'Unicode': 'U+1F345'}, {'NomeReact': 'beb9b037310f291f61d07fcb23d817a3', 'Unicode': 'U+1F347'}, {'NomeReact': '6c14e77686f11ad6b862169a0519d473', 'Unicode': 'U+1F349'}, {'NomeReact': 'a5f1edef4ebec69590eeea09f25ea77a', 'Unicode': 'U+1F34B'}, {'NomeReact': 'ebbcea75753017be8c708f555d3412ee', 'Unicode': 'U+1F34C'}, {'NomeReact': 'ab7074b4cf2531ff281c6c791a17282f', 'Unicode': 'U+1F34D'}, {'NomeReact': 'fd4eb04d5ce30217bd07f5f82dedec62', 'Unicode': 'U+1F34E'}, {'NomeReact': '4017af23a67ab7c4a95659cb5414e3d9', 'Unicode': 'U+1F350'}, {'NomeReact': 'fc847896978cd4aac4b9a04d5e33ddac', 'Unicode': 'U+1F351'}, {'NomeReact': '1d34de8270521d4f21bdc416441aaf56', 'Unicode': 'U+1F352'}, {'NomeReact': '37241ba41434aaedbb3048e2a6893c9a', 'Unicode': 'U+1F353'}, {'NomeReact': 'c400cd5e4a61bbc1f0ff99340bd630fc', 'Unicode': 'U+1F354'}, {'NomeReact': 'fc5dfee17517b60efee0f528c0a8dd0a', 'Unicode': 'U+1F355'}, {'NomeReact': '15eee1c6a251abb3d355d18aa1cb7d1d', 'Unicode': 'U+1F357'}, {'NomeReact': '8a4f13d6840859d0f30b7950d89f67a6', 'Unicode': 'U+1F363'}, {'NomeReact': '8676aa4b711efb36e347a538bb623f09', 'Unicode': 'U+1F36C'}, {'NomeReact': '8f08694fda9bedac20736b369bb46f53', 'Unicode': 'U+1F373'}, {'NomeReact': '8d12327a748892b45e82b1029be62f17', 'Unicode': 'U+1F377'}, {'NomeReact': '536b0c3ce0221a82362913867414582c', 'Unicode': 'U+1F37A'}, {'NomeReact': 'cd3c87f93e5d83c2ff769e38f1bd3a2c', 'Unicode': 'U+1F37D'}, {'NomeReact': 'd954853ac4aea7965c4f12cf56a32b52', 'Unicode': 'U+1F381'}, {'NomeReact': 'a2f5313b2aa67d5bbe860a849f3308f1', 'Unicode': 'U+1F382'}, {'NomeReact': '54a2c4daa82e7f9a35811ea13e960322', 'Unicode': 'U+1F385'}, {'NomeReact': 'a9316af299874f59f512fbff442fb156', 'Unicode': 'U+1F38A'}, {'NomeReact': '85e052bdc3a973e60b35040b192e8ae0', 'Unicode': 'U+1F3A2'}, {'NomeReact': '9843e49b8124f1f99d821889dcb28765', 'Unicode': 'U+1F3A3'}, {'NomeReact': '7a76709bd5ba650cf7dd0af4779be357', 'Unicode': 'U+1F3A4'}, {'NomeReact': 'bab229b296e0334babe270e3d137bd11', 'Unicode': 'U+1F3A8'}, {'NomeReact': 'b61c6dd5247694977ca629949b0bbb1f', 'Unicode': 'U+1F3AC'}, {'NomeReact': '43650b1a7dfa8d8d983e988315266517', 'Unicode': 'U+1F3AD'}, {'NomeReact': '4628459e30811e799e051738561c634a', 'Unicode': 'U+1F3AE'}, {'NomeReact': '1cfcbd0573b2ea5832e0bac8790ec755', 'Unicode': 'U+1F3B1'}, {'NomeReact': '7aa51bea7b9f14b2f5d18e14a64edb87', 'Unicode': 'U+1F3B2'}, {'NomeReact': 'e11b9365333698ec4f146ea53a2f4a26', 'Unicode': 'U+1F3B6'}, {'NomeReact': '06aa9836aaf3bc0fb0a7761833c4e08e', 'Unicode': 'U+1F3BE'}, {'NomeReact': '90e18720639c46f741b42a14dbbed9dc', 'Unicode': 'U+1F3C1'}, {'NomeReact': 'bf603fa06e13eefc4eb32ef8535ebc9f', 'Unicode': 'U+1F3C3'}, {'NomeReact': '06aa705f9442a85d0cf93ebac8e4f7c7', 'Unicode': 'U+1F3C3'}, {'NomeReact': '43e47f54b10f5c1d59e6e12cd0f18f31', 'Unicode': 'U+1F3C4'}, {'NomeReact': 'ee5c20e090c69e11d09523e8a53b2e95', 'Unicode': 'U+1F3C6'}, {'NomeReact': '39ebda42d074fe7c2f51125e5edefff2', 'Unicode': 'U+1F3C8'}, {'NomeReact': 'cfce0260d4f8630076007368a21239e5', 'Unicode': 'U+1F3CB'}, {'NomeReact': '2edf96d835885ecfd3dc54b51d9d090b', 'Unicode': 'U+1F3D0'}, {'NomeReact': '733b5ad4b3a5ea89af780d77a361fc58', 'Unicode': 'U+1F3D3'}, {'NomeReact': 'f2051894f34af8949eb275999a106862', 'Unicode': 'U+1F3D5'}, {'NomeReact': '328aca23cb0eb4ae02c23c5547f7abb0', 'Unicode': 'U+1F3D6'}, {'NomeReact': '81710f56ab4c90352af7ec396106a2b0', 'Unicode': 'U+1F3DB'}, {'NomeReact': '6f32ef827751d69729874c0de5997c55', 'Unicode': 'U+1F3DC'}, {'NomeReact': 'a922a901cee0a3d8d6d2a3766492f4cb', 'Unicode': 'U+1F3DE'}, {'NomeReact': 'aee4b2d3822baf7264f9a1c20d45c166', 'Unicode': 'U+1F3DF'}, {'NomeReact': '9d5ca73f1e0c866e97e111b5fb85d343', 'Unicode': 'U+1F3EA'}, {'NomeReact': '89ef92975b6a1dad6d38737cded6c17a', 'Unicode': 'U+1F40C'}, {'NomeReact': '2fbfc6902bb0151c93105bb679fbcf04', 'Unicode': 'U+1F40D'}, {'NomeReact': 'abf199e086952764477281311bf91299', 'Unicode': 'U+1F414'}, {'NomeReact': '4fda812ae8e044d35b0fbdbd5c0c3217', 'Unicode': 'U+1F418'}, {'NomeReact': '0436c6ff98574e660574d740bed3c1d4', 'Unicode': 'U+1F420'}, {'NomeReact': 'd0e550faed2f5b1239818e4ae0cfa998', 'Unicode': 'U+1F422'}, {'NomeReact': '3b577571ba7f4e19339f2f4c60c03fbb', 'Unicode': 'U+1F424'}, {'NomeReact': '7f9fd9924b490f3dbddec1bb892f6bd7', 'Unicode': 'U+1F427'}, {'NomeReact': '2090eb7f4c343f29c59386b04100ec9c', 'Unicode': 'U+1F428'}, {'NomeReact': '17460c3ee293e14f0d655887ff7a9cd8', 'Unicode': 'U+1F42D'}, {'NomeReact': 'e31177cc632831a3c50602ea30ff9ed6', 'Unicode': 'U+1F42E'}, {'NomeReact': 'c0dafdfe408944f7e7d086a24947dd7e', 'Unicode': 'U+1F42F'}, {'NomeReact': 'da6c718cead6440a2a38f4edfd686584', 'Unicode': 'U+1F430'}, {'NomeReact': '0da22bb7f6b1d18fccfbe72fa6d49b76', 'Unicode': 'U+1F431'}, {'NomeReact': 'b08dbd34a4ab1beeaa7d5ce54d2bc5e0', 'Unicode': 'U+1F433'}, {'NomeReact': '7ac865271a35d8dec3a159cfba5900c9', 'Unicode': 'U+1F435'}, {'NomeReact': '8f0d8170d8741870848e4a7b5d8a63b3', 'Unicode': 'U+1F436'}, {'NomeReact': '95a3724aee6b0c1e9fb4b91503de18d5', 'Unicode': 'U+1F437'}, {'NomeReact': '656824707cfc30509fc55069f227aaf0', 'Unicode': 'U+1F438'}, {'NomeReact': '4f0c419d36e76062b02b7a5fb51905b7', 'Unicode': 'U+1F43B'}, {'NomeReact': '767437727d25abee732bb3f2fa25e10b', 'Unicode': 'U+1F43C'}, {'NomeReact': '16a247ebd0fbeed8f16f253fe6242d34', 'Unicode': 'U+1F446'}, {'NomeReact': '91dd290cd0e32d3549b7c1ea0b23ccdb', 'Unicode': 'U+1F447'}, {'NomeReact': '0ace2d597128177f4eeb105bc71a3dd5', 'Unicode': 'U+1F448'}, {'NomeReact': '3941c1c274d6743fdd94c15aa2682f21', 'Unicode': 'U+1F449'}, {'NomeReact': 'c04e88ff60542e4190a6429ab477feed', 'Unicode': 'U+1F44A'}, {'NomeReact': 'd4f9c03e051c07a0b5042656cc867c7f', 'Unicode': 'U+1F44D'}, {'NomeReact': 'db58f075d4a161177af81f7e548ffbd7', 'Unicode': 'U+1F44E'}, {'NomeReact': 'fb45ad6fdb1c1dd290004faeffb7d5f4', 'Unicode': 'U+1F44F'}, {'NomeReact': 'e3bbff50a0ea6765f161b6a78498d103', 'Unicode': 'U+1F46F'}, {'NomeReact': '638e394fe86d33c2e41ea90205bce46e', 'Unicode': 'U+1F476'}, {'NomeReact': '94b916f11cfea36d3ffe89091b9fdfbb', 'Unicode': 'U+1F47B'}, {'NomeReact': '388fe19ed75de2ed33d3b354cabc31f4', 'Unicode': 'U+1F47B'}, {'NomeReact': '28eb3374ca0a632c623919ef7263d93f', 'Unicode': 'U+1F47D'}, {'NomeReact': '487045dfbab62b3ab8cc47c03e35ac05', 'Unicode': 'U+1F480'}, {'NomeReact': 'e41ac299cd5ade85c49c32d9903b7e98', 'Unicode': 'U+1F481'}, {'NomeReact': '4d1ada000444cdf737d9d5681b255792', 'Unicode': 'U+1F481'}, {'NomeReact': '8979eab76a520505608395fb37638768', 'Unicode': 'U+1F483'}, {'NomeReact': 'ede014192b4d22a24e55699e199249be', 'Unicode': 'U+1F485'}, {'NomeReact': 'c56ab262da065ee6665fb8c14e8b8785', 'Unicode': 'U+1F48B'}, {'NomeReact': '763c01a5ba85d6b9f997a4748bca04d9', 'Unicode': 'U+1F493'}, {'NomeReact': '590ca79727ac32e143405a8aadd20a4e', 'Unicode': 'U+1F494'}, {'NomeReact': 'bbcebc0947d132d5779f86d8ba678684', 'Unicode': 'U+1F499'}, {'NomeReact': '26190f30566fa35da8e48d458923331e', 'Unicode': 'U+1F49B'}, {'NomeReact': 'cf8df76b4db8b2a4f69113f54aa46f19', 'Unicode': 'U+1F4A6'}, {'NomeReact': '2860e70b9da726a09e62cf425760eafb', 'Unicode': 'U+1F4A8'}, {'NomeReact': '05cd7c4849c66edb6730646f1c5ed9a1', 'Unicode': 'U+1F4A9'}, {'NomeReact': '882c831b685c40d596d4cdfc8cd90854', 'Unicode': 'U+1F4AA'}, {'NomeReact': 'd6c94b2b1a1a78e01c1ad941a7118b55', 'Unicode': 'U+1F4AF'}, {'NomeReact': 'ec56fedf90673317d3177e2eb94c7507', 'Unicode': 'U+1F4B0'}, {'NomeReact': '6c0278acb0926380dde8f1cc9876e49e', 'Unicode': 'U+1F4B3'}, {'NomeReact': '362e12889f89ad3188987d2a25d604fa', 'Unicode': 'U+1F4F1'}, {'NomeReact': '854ffa007672833e10c80892b8895985', 'Unicode': 'U+1F4F6'}, {'NomeReact': '76a3511ae1de5bcdae2f71670873e78c', 'Unicode': 'U+1F4F6'}, {'NomeReact': '49dcc34aa305efd97463a09fd9e4117e', 'Unicode': 'U+1F4F6'}, {'NomeReact': '93747a967df3a1bd5c0285b159885afb', 'Unicode': 'U+1F4F6'}, {'NomeReact': '9e66ea916653ffee2d989c5d1e6ec8a0', 'Unicode': 'U+1F4F7'}, {'NomeReact': 'ac73c456f8fc2a2cb43cc987074dca6b', 'Unicode': 'U+1F4FA'}, {'NomeReact': 'c892bd3e2bfaa915a4176220ad4f3a03', 'Unicode': 'U+1F4FB'}, {'NomeReact': '88d9ac43b167b12832acf9cf01ffd2a7', 'Unicode': 'U+1F507'}, {'NomeReact': '4367ef1b7bf1fa26a29d427d791ec77d', 'Unicode': 'U+1F50B'}, {'NomeReact': '4fca92e314a748bd517fadd71fe979d7', 'Unicode': 'U+1F51D'}, {'NomeReact': '64c605f4a7efe2216a378bd88917bf87', 'Unicode': 'U+1F525'}, {'NomeReact': 'f946a7255e2b9e819dc1e8febcab3ea0', 'Unicode': 'U+1F526'}, {'NomeReact': '95c0cacf4af0632e43618a21f6fdfb5a', 'Unicode': 'U+1F52B'}, {'NomeReact': 'ffd8544d8af707e52818c47b3f499faa', 'Unicode': 'U+1F575'}, {'NomeReact': 'eddcd6e245a736d88da6787b91fcbfb0', 'Unicode': 'U+1F577'}, {'NomeReact': '955dcd7efc49f4676f27130d1114721a', 'Unicode': 'U+1F596'}, {'NomeReact': 'f9a77826acc4bb229ee3b60071a96bb6', 'Unicode': 'U+1F5A5'}, {'NomeReact': '005968b99f845dfd469e522bb5df941a', 'Unicode': 'U+1F5A8'}, {'NomeReact': '2092505894976b47538b5ea6e315aa63', 'Unicode': 'U+1F5FB'}, {'NomeReact': 'afc6de35a5a7c17c2650c99d9dddfa18', 'Unicode': 'U+1F5FC'}, {'NomeReact': '94ab61fdef549fa734b86614ddda9cc9', 'Unicode': 'U+1F5FD'}, {'NomeReact': '500322f52c98b484f599422359c12a58', 'Unicode': 'U+1F600'}, {'NomeReact': 'e8c454da5b77cf7412b38e50ce829602', 'Unicode': 'U+1F602'}, {'NomeReact': '30c6dae375491f3da7747d10b0903b7e', 'Unicode': 'U+1F603'}, {'NomeReact': '4b07e2c96f8e164ac6fa917a3d492ecf', 'Unicode': 'U+1F604'}, {'NomeReact': 'ad71f12f64f776608e352ab7fb67be43', 'Unicode': 'U+1F604'}, {'NomeReact': '3632226c32869edf1708fd8d65343493', 'Unicode': 'U+1F605'}, {'NomeReact': '6f4ecca59760854480d30bfca8f3cb92', 'Unicode': 'U+1F606'}, {'NomeReact': '2b6c95bee7505c6e6793785cef5935ef', 'Unicode': 'U+1F607'}, {'NomeReact': '985285c5791e299632f18d0858d66665', 'Unicode': 'U+1F608'}, {'NomeReact': 'b06f3d98aaeca318f9ba8ed47b09de1f', 'Unicode': 'U+1F608'}, {'NomeReact': '9adc515c4a04376dc20289ec5ef80361', 'Unicode': 'U+1F60D'}, {'NomeReact': 'ef6276b501803ffc6ae9b8ac7067b440', 'Unicode': 'U+1F60E'}, {'NomeReact': '79e987041117276334c6546b1f103bc9', 'Unicode': 'U+1F618'}, {'NomeReact': '8b808da087f4facaf94a1c19748b9137', 'Unicode': 'U+1F61B'}, {'NomeReact': '2d7af04cc5f85ebe91d06719e21698ea', 'Unicode': 'U+1F61C'}, {'NomeReact': '73c4fdd5c107f8c6db7ccf34b54e5d1d', 'Unicode': 'U+1F61E'}, {'NomeReact': 'b2b1af57456385fdd492802fa2cfd3aa', 'Unicode': 'U+1F620'}, {'NomeReact': '1ddaee09e4bf7a8ffe7d0504d801171f', 'Unicode': 'U+1F621'}, {'NomeReact': '6a0cd604066589d13cd312e66d571841', 'Unicode': 'U+1F622'}, {'NomeReact': '9a97b843c0dc1e0a0c931cee405dce53', 'Unicode': 'U+1F624'}, {'NomeReact': '0d08649948fac3075b2713a314859c0a', 'Unicode': 'U+1F62A'}, {'NomeReact': '5d985d09e0a1a81c82aa6693822a95a2', 'Unicode': 'U+1F62C'}, {'NomeReact': '15be2f11d88be9228df4479fca78b7cf', 'Unicode': 'U+1F62C'}, {'NomeReact': 'ed6fd69f86d9a8b9fffb5ca6f16f7786', 'Unicode': 'U+1F62D'}, {'NomeReact': '8dc055967ad00d019100adb5c8068e01', 'Unicode': 'U+1F630'}, {'NomeReact': '4aa46a3eca576f540cd274f1c768ac59', 'Unicode': 'U+1F631'}, {'NomeReact': '19ed4cf948a888b4196b625206cbec8b', 'Unicode': 'U+1F632'}, {'NomeReact': 'e457330be549ef95d7f66aaaf3778ff2', 'Unicode': 'U+1F633'}, {'NomeReact': '4d23c0359eef25a06e418315c49814b2', 'Unicode': 'U+1F634'}, {'NomeReact': '9ed78925b5c4c2d270ebe033daff97c1', 'Unicode': 'U+1F635'}, {'NomeReact': 'e68b80f7ff8cc3767586a71aa1c5636e', 'Unicode': 'U+1F637'}, {'NomeReact': '91a400b4648ae362dbf21c1c7851e61b', 'Unicode': 'U+1F63A'}, {'NomeReact': '58645caef568fe9164092bb49c49f15d', 'Unicode': 'U+1F63B'}, {'NomeReact': '2dbe0f4c30a8121733808bafd202a455', 'Unicode': 'U+1F644'}, {'NomeReact': '3a6f8bf6d2cf3c95ea7ef78c63fcdc6b', 'Unicode': 'U+1F648'}, {'NomeReact': '49acbd8985bec0a033b65bce6b54a1d1', 'Unicode': 'U+1F649'}, {'NomeReact': '40a3ef7cbd2f2a0bb9957b5357ea39b9', 'Unicode': 'U+1F64A'}, {'NomeReact': '832c809998c7dbebaaac85957a99ddd8', 'Unicode': 'U+1F64B'}, {'NomeReact': '9ac8ba8cee699896352897de282432c9', 'Unicode': 'U+1F64C'}, {'NomeReact': '2262052e80215dd4fae312f39494e84f', 'Unicode': 'U+1F64F'}, {'NomeReact': 'bcdd7f14a1020f0b05838adc6e12432a', 'Unicode': 'U+1F680'}, {'NomeReact': '1b98770f53eb1fc2926fa140646a443a', 'Unicode': 'U+1F681'}, {'NomeReact': 'dbaf7bea199d8be7f6b465eb0ef7b8ed', 'Unicode': 'U+1F687'}, {'NomeReact': 'b4de642eaba06c102c3b6160f7d2ec25', 'Unicode': 'U+1F68A'}, {'NomeReact': 'e070e2e00fa1320c9be1d14ef19009ea', 'Unicode': 'U+1F68D'}, {'NomeReact': 'fde04ffdc147e1204a8e42e1a141a105', 'Unicode': 'U+1F691'}, {'NomeReact': '728744a3144a49e85d964003b5f0c088', 'Unicode': 'U+1F693'}, {'NomeReact': 'dac68151727d6487a17b2f46263710ee', 'Unicode': 'U+1F695'}, {'NomeReact': 'bf059cc24f2953a8306c418d8cef9db6', 'Unicode': 'U+1F697'}, {'NomeReact': 'ef847d1b1663cb2a7bdafc9eb3f2bbb9', 'Unicode': 'U+1F6AC'}, {'NomeReact': '8b5ccbb0b46fcb0b762233a8d8b44df2', 'Unicode': 'U+1F6B4'}, {'NomeReact': 'd04e69c2aaaf74e1cd08ca9aaf3952ae', 'Unicode': 'U+1F6B6'}, {'NomeReact': '544c34bb89134e8a36296d457eb631ad', 'Unicode': 'U+1F6CC'}, {'NomeReact': '72d785472827ce8850f9c268e408be9a', 'Unicode': 'U+1F6F3'}, {'NomeReact': '21b5821260d1b9061e861ef3b4300d9f', 'Unicode': 'U+1F910'}, {'NomeReact': '81ed2cb94a66636b2d97c9df8e16585a', 'Unicode': 'U+1F911'}, {'NomeReact': '2e232b8240e8b34ff9488b7583becd23', 'Unicode': 'U+1F912'}, {'NomeReact': '748d0a1b0d320c1e3ea8fbb79177d706', 'Unicode': 'U+1F913'}, {'NomeReact': 'fa53e0b04d3959e8cb5956526fb474cc', 'Unicode': 'U+1F914'}, {'NomeReact': '9e63d3effdff0a75539cd8e39a394e1c', 'Unicode': 'U+1F916'}, {'NomeReact': '762583eb4e8d41e8d748b28087fc5203', 'Unicode': 'U+1F917'}, {'NomeReact': '24c04c18153417cc4acad7f944419d2f', 'Unicode': 'U+1F918'}, {'NomeReact': '1bec1ef5a42feb16498a387128ecea82', 'Unicode': 'U+1F91D'}, {'NomeReact': 'ed6bd728fc162c76e44e694c96642f01', 'Unicode': 'U+1F981'}, {'NomeReact': '6d1ed2a60a75695f50f5a11bbc562d3a', 'Unicode': 'U+1F98A'}, {'NomeReact': '47454befeefd7f6605d9e79b157e081b', 'Unicode': 'U+1FA82'}]

# Converter os valores do dict_emotions para emojis
dict_emotions = {item["NomeReact"]: chr(int(item["Unicode"][2:], 16)) for item in dict_emotions}


pipeline = beam.Pipeline()



# Crie uma PCollection
pcoll = pipeline | "Create PCollection" >> beam.Create([
            (1, 'ddd', 25,'<img class="react-emoji" src="./assets/upload/react/30c6dae375491f3da7747d10b0903b7e.png" title=":grinningface:" alt=":grinningface:"/>'), 
            (2, 'ccc', 30,'<img class="react-emoji" src="./assets/upload/react/fb45ad6fdb1c1dd290004faeffb7d5f4.png" title=":clap:" alt=":clap:"/>'), 
            (3, ' d', 35,'<img class="react-emoji" src="./assets/upload/react/fb45ad6fdb1c1dd290004faeffb7d5f4.png" title=":clap:" alt=":clap:"/> <img class="react-emoji" src="./assets/upload/react/fb45ad6fdb1c1dd290004faeffb7d5f4.png" title=":clap:" alt=":clap:"/> <img class="react-emoji" src="./assets/upload/react/d4f9c03e051c07a0b5042656cc867c7f.png" title=":thumbsup:" alt=":thumbsup:"/> <img class="react-emoji" src="./assets/upload/react/d4f9c03e051c07a0b5042656cc867c7f.png" title=":thumbsup:" alt=":thumbsup:"/> <img class="react-emoji" src="./assets/upload/react/fb45ad6fdb1c1dd290004faeffb7d5f4.png" title=":clap:" alt=":clap:"/> <img class="react-emoji" src="./assets/upload/react/d4f9c03e051c07a0b5042656cc867c7f.png" title=":thumbsup:" alt=":thumbsup:"/> <img class="react-emoji" src="./assets/upload/react/d4f9c03e051c07a0b5042656cc867c7f.png" title=":thumbsup:" alt=":thumbsup:"/> <img class="react-emoji" src="./assets/upload/react/d4f9c03e051c07a0b5042656cc867c7f.png" title=":thumbsup:" alt=":thumbsup:"/> <img class="react-emoji" src="./assets/upload/react/fb45ad6fdb1c1dd290004faeffb7d5f4.png" title=":clap:" alt=":clap:"/> <img class="react-emoji" src="./assets/upload/react/762583eb4e8d41e8d748b28087fc5203.png" title=":hugging_face:" alt=":hugging_face:"/> <img class="react-emoji" src="./assets/upload/react/762583eb4e8d41e8d748b28087fc5203.png" title=":hugging_face:" alt=":hugging_face:"/> <img class="react-emoji" src="./assets/upload/react/fb45ad6fdb1c1dd290004faeffb7d5f4.png" title=":clap:" alt=":clap:"/> <img class="react-emoji" src="./assets/upload/react/fb45ad6fdb1c1dd290004faeffb7d5f4.png" title=":clap:" alt=":clap:"/> <img class="react-emoji" src="./assets/upload/react/762583eb4e8d41e8d748b28087fc5203.png" title=":hugging_face:" alt=":hugging_face:"/> <img class="react-emoji" src="./assets/upload/react/762583eb4e8d41e8d748b28087fc5203.png" title=":hugging_face:" alt=":hugging_face:"/> <img class="react-emoji" src="./assets/upload/react/fb45ad6fdb1c1dd290004faeffb7d5f4.png" title=":clap:" alt=":clap:"/> <img class="react-emoji" src="./assets/upload/react/fb45ad6fdb1c1dd290004faeffb7d5f4.png" title=":clap:" alt=":clap:"/> <img class="react-emoji" src="./assets/upload/react/d4f9c03e051c07a0b5042656cc867c7f.png" title=":thumbsup:" alt=":thumbsup:"/> <img class="react-emoji" src="./assets/upload/react/d4f9c03e051c07a0b5042656cc867c7f.png" title=":thumbsup:" alt=":thumbsup:"/> <img class="react-emoji" src="./assets/upload/react/d4f9c03e051c07a0b5042656cc867c7f.png" title=":thumbsup:" alt=":thumbsup:"/> <img class="react-emoji" src="./assets/upload/react/fb45ad6fdb1c1dd290004faeffb7d5f4.png" title=":clap:" alt=":clap:"/>')
          ])
pcoll = pcoll | "Name Columns" >> beam.Map(lambda row: {"id": row[0], "name": row[1], "age": row[2], "text": row[3]})

# Aplica a transformação nos dados
# Aplica a transformação nos dados

resultados = (
    pcoll
    |"Extract Emojis" >> beam.ParDo(ExtractTagsTransform(column_names, dict_emotions))
    | beam.Map(print)
)
    
# Executa o pipeline
resultados = pipeline.run().wait_until_finish()




import re
import apache_beam as beam

def extraiMentions(colMentions):
    colMentions = str(colMentions) 
    mentions =  mentions = re.findall(r'(\s*<a\b[^>]*class=[\"]mentions[^>]*>(.*?)<\/a>\s*)',colMentions) #re.findall(r'(\s*<a\b[^>]*class=[\"]mentions[^>]*>(.*?)<\/a>\s*)', coluna)
    
    for mention in mentions:
        link, user = mention
        colMentions = re.sub(re.escape(link), f' {user} ', str(colMentions))
    
    return colMentions

# Função de transformação para aplicar a função extraiMentions em cada elemento
class ExtraiMentionsTransform(beam.DoFn):
    def process(self, element):
        colMentions = element
        coluna_processada = extraiMentions(colMentions)
        yield coluna_processada


# Cria um pipeline do Apache Beam
pipeline = beam.Pipeline()

# Dados de exemplo
dados = [
    (1, "Texto 1", "<a class=\"mentions\" href=\"#\">John</a> mentioned <a class=\"mentions\" href=\"#\">Mary</a>"),
    (2, "Texto 2", "No mentions in this text."),
    (3, "Texto 3", "Another <a class=\"mentions\" href=\"#\">mention</a> example <a class=\"mentions\" href=\"#\">here</a>.")
]

# Crie uma PCollection
pcoll = pipeline | "Create PCollection" >> beam.Create([
            (1, 'John', 25,'ooo'), 
            (2, '<a class="mentions userinfo" data-id-user="71428"><a class="mentions userinfo" data-id-user="71277">@ana.nunes</a></a> Conseguis-te logar-te sem o checkpoint?', 30,'aaa'), 
            (3, ' <a class="mentions userinfo" data-id-user="330038"><a class="mentions userinfo" data-id-user="329979">@tvpinto</a></a>  Barcelos Advogados Associados', 35,'bbb'),
            (4, ' <a class="mentions userinfo" data-id-user="330039"><a class="mentions userinfo" data-id-user="329980">@tvpinto</a></a>  Barcelos Advogados Associados', 35,'<a class="mentions userinfo" data-id-user="330039"><a class="mentions userinfo" data-id-user="329980">@tvpinto</a></a>  Barcelos Advogados Associados')])
pcoll = pcoll | "Name Columns" >> beam.Map(lambda row: {"id": row[0], "name": row[1], "age": row[2], "aleatorio": row[3]})

# Aplica a transformação nos dados
# Aplica a transformação nos dados
resultados = (
    pcoll
    | beam.ParDo(ExtraiMentionsTransform())  # Aplica a transformação em cada elemento
    | beam.Map(print)
)
    
# Executa o pipeline
resultados = pipeline.run().wait_until_finish()

# Imprime os resultados
#for resultado in resultados:
    #print(resultado)



















def extraiMentions(colMentions):
#    """
#     Descrição da função: Esta função lê a tabela SELECT * FROM `focus-mechanic-321819.beegdata_engineer.lista_emojis`
#     e trata as colunas predefinidas no pipeline do arquivos onde contém class=[\"]mentions  alterando a string da coluna de forma que o resultado seja um emoji.
#     """
    colMentions = str(colMentions) 
    mentions =  mentions = re.findall(r'(\s*<a\b[^>]*class=[\"]mentions[^>]*>(.*?)<\/a>\s*)',colMentions) #re.findall(r'(\s*<a\b[^>]*class=[\"]mentions[^>]*>(.*?)<\/a>\s*)', coluna)
    
    for mention in mentions:
        link, user = mention
        colMentions = re.sub(re.escape(link), f' {user} ', str(colMentions))
    
    return colMentions

# Função de transformação para aplicar a função extraiMentions em cada elemento
class ExtraiMentionsTransform(beam.DoFn):
    def process(self, element):
        colMentions = element
        coluna_processada = extraiMentions(colMentions)
        yield coluna_processada


# Cria um pipeline do Apache Beam
pipeline = beam.Pipeline()

# Dados de exemplo
dados = [
    (1, "Texto 1", "<a class=\"mentions\" href=\"#\">John</a> mentioned <a class=\"mentions\" href=\"#\">Mary</a>"),
    (2, "Texto 2", "No mentions in this text."),
    (3, "Texto 3", "Another <a class=\"mentions\" href=\"#\">mention</a> example <a class=\"mentions\" href=\"#\">here</a>.")
]

# Crie uma PCollection
pcoll = pipeline | "Create PCollection" >> beam.Create([
            (1, 'John', 25,'ooo'), 
            (2, '<a class="mentions userinfo" data-id-user="71428"><a class="mentions userinfo" data-id-user="71277">@ana.nunes</a></a> Conseguis-te logar-te sem o checkpoint?', 30,'aaa'), 
            (3, ' <a class="mentions userinfo" data-id-user="330038"><a class="mentions userinfo" data-id-user="329979">@tvpinto</a></a>  Barcelos Advogados Associados', 35,'bbb'),
            (4, ' <a class="mentions userinfo" data-id-user="330039"><a class="mentions userinfo" data-id-user="329980">@tvpinto</a></a>  Barcelos Advogados Associados', 35,'<a class="mentions userinfo" data-id-user="330039"><a class="mentions userinfo" data-id-user="329980">@tvpinto</a></a>  Barcelos Advogados Associados')])
pcoll = pcoll | "Name Columns" >> beam.Map(lambda row: {"id": row[0], "name": row[1], "age": row[2], "aleatorio": row[3]})

# Aplica a transformação nos dados
# Aplica a transformação nos dados
resultados = (
    pcoll
    | beam.ParDo(ExtraiMentionsTransform())  # Aplica a transformação em cada elemento
    | beam.Map(print)
)
    
# Executa o pipeline
resultados = pipeline.run().wait_until_finish()