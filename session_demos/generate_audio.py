import os, io
from gtts import gTTS
from pydub import AudioSegment

OUT_DIR = os.path.join(os.path.dirname(__file__), 'audio_files')
os.makedirs(OUT_DIR, exist_ok=True)

PAUSE = AudioSegment.silent(duration=600)
SHORT_PAUSE = AudioSegment.silent(duration=300)

CALLS = [
    {
        "filename": "call_001_licence_complaint.wav",
        "turns": [
            ("agent", "en", "com.au", "Good afternoon, thank you for calling Queensland Transport and Main Roads, this is Sarah Chen speaking, how can I help you today?"),
            ("caller", "en", "co.uk", "Yeah hi Sarah, my name is James Wilson, I'm calling because I'm absolutely fed up with how long my licence renewal is taking. I submitted everything three weeks ago and I still haven't heard back."),
            ("agent", "en", "com.au", "I'm sorry to hear about the delay Mister Wilson. Let me look into that for you. Could you please confirm your date of birth and your licence number?"),
            ("caller", "en", "co.uk", "Sure, it's the fifteenth of March nineteen eighty two, and my licence number is zero four seven three nine eight two one."),
            ("agent", "en", "com.au", "Thank you James. I can see your application here. It looks like there was an issue with the medical certificate that was submitted. We sent a letter to your address at twenty seven Maple Street, Toowong, four zero six six, on the ninth of April."),
            ("caller", "en", "co.uk", "What? I never received any letter. Nobody called me either. This is ridiculous. I need my licence for work, I drive trucks for a living."),
            ("agent", "en", "com.au", "I completely understand your frustration and I apologise for the inconvenience. Let me escalate this to our processing team right now. I can also update your contact details. What's the best phone number to reach you on?"),
            ("caller", "en", "co.uk", "It's oh four one two, three four five, six seven eight. And my email is james dot wilson at gmail dot com. I just want this sorted."),
            ("agent", "en", "com.au", "Absolutely. I've flagged this as urgent and you should receive a call back within twenty four hours. I've also noted your phone number and email. Is there anything else I can help you with today?"),
            ("caller", "en", "co.uk", "No, that's it. Just please make sure someone actually calls me back this time."),
            ("agent", "en", "com.au", "You have my word, Mister Wilson. Your reference number is T M R dash two zero two six dash zero four one five eight. Thank you for your patience."),
        ]
    },
    {
        "filename": "call_002_registration_thankyou.wav",
        "turns": [
            ("agent", "en", "com.au", "Good morning, Queensland Transport and Main Roads, David Kumar speaking."),
            ("caller", "en", "com.au", "Hi David! This is Emily Brown calling. I just wanted to say a huge thank you. I transferred my car registration from New South Wales last week and the whole process was incredibly smooth."),
            ("agent", "en", "com.au", "Oh that's wonderful to hear Emily, thank you for calling to let us know! Can I ask which office handled your transfer?"),
            ("caller", "en", "com.au", "It was the Southport office on the Gold Coast. The staff there were so helpful. I was in and out in twenty minutes, and my new Queensland registration arrived in the mail yesterday."),
            ("agent", "en", "com.au", "That's great feedback. I'll make sure to pass that along to the Southport team. Is your new registration number Q L D seven eight three Alpha Bravo?"),
            ("caller", "en", "com.au", "Yes that's the one! Everything looks perfect. My address is forty two Ocean Parade, Broadbeach, four two one eight. All the details are correct."),
            ("agent", "en", "com.au", "Excellent. I'm glad we could make the transition seamless for you. Is there anything else you need help with?"),
            ("caller", "en", "com.au", "Nope, all sorted. You guys have been fantastic. Have a great day!"),
            ("agent", "en", "com.au", "Thank you Emily, you too! Welcome to Queensland."),
        ]
    },
    {
        "filename": "call_003_elderly_portal_help.wav",
        "turns": [
            ("agent", "en", "com.au", "Good afternoon, Transport and Main Roads, Lisa Nguyen here. How may I help you?"),
            ("caller", "en", "co.uk", "Oh hello dear, my name is Margaret O'Brien. I'm trying to use that online portal thing to renew my registration but I keep getting stuck. My grandson set it up for me but I can't remember how to log in."),
            ("agent", "en", "com.au", "No worries at all Margaret, I'd be happy to help you through it. Are you sitting in front of your computer right now?"),
            ("caller", "en", "co.uk", "Yes I am. I'm on that Queensland government website. There's so many buttons I don't know which one to press."),
            ("agent", "en", "com.au", "That's perfectly fine. First, can you see a blue button at the top right that says sign in?"),
            ("caller", "en", "co.uk", "Oh yes, I think I see it. Let me put my glasses on. Yes, sign in, I can see it now."),
            ("agent", "en", "com.au", "Great, go ahead and click that. Then you'll need your Queensland digital identity. Do you remember your email address you signed up with?"),
            ("caller", "en", "co.uk", "I think it was margaret dot obrien at bigpond dot com. My grandson set that up for me too. I'm seventy eight years old, this technology is all very confusing."),
            ("agent", "en", "com.au", "You're doing really well Margaret. There's no rush at all. Now, if you've forgotten your password, there should be a link that says forgot password underneath the login box."),
            ("caller", "en", "co.uk", "Oh I see it! Should I click that? My registration is for my little Toyota Corolla, registration number A B C one two three. It expires next week and my grandson said I need to do it online."),
            ("agent", "en", "com.au", "Yes click that link and it will send a new password to your email. And don't worry, if you prefer, you can also renew in person at your local transport office in Toowoomba. Would you like me to help you find the closest one?"),
            ("caller", "en", "co.uk", "Oh that might be easier actually. I live at fifteen Rose Street, East Toowoomba. Is there one nearby?"),
            ("agent", "en", "com.au", "Yes! There's a transport office right on Neil Street in the Toowoomba city centre, about ten minutes from you. They're open Monday to Friday nine to four thirty."),
            ("caller", "en", "co.uk", "Oh lovely, I'll get my neighbour to drive me there. Thank you so much dear, you've been very patient with me."),
            ("agent", "en", "com.au", "It's my absolute pleasure Margaret. Don't hesitate to call us anytime you need help."),
        ]
    },
    {
        "filename": "call_004_bilingual_mandarin.wav",
        "turns": [
            ("agent", "en", "com.au", "Good morning, Transport and Main Roads, Lucy Wang speaking. How can I help?"),
            ("caller", "zh-cn", "com", "你好，我叫张伟。我需要帮助处理我的车辆过户。我的英语不太好，你能说中文吗？"),
            ("agent", "zh-cn", "com", "你好张先生，我可以说中文。请告诉我，您需要什么帮助？"),
            ("caller", "zh-cn", "com", "太好了！我刚从悉尼搬到布里斯班。我需要把我的新南威尔士州车辆登记转到昆士兰州。我的地址是布里斯班 Sunnybank 的 88 号 Mains Road，邮编 4109。"),
            ("agent", "zh-cn", "com", "好的，张先生。您需要带几样东西到我们的办公室。首先是您的新南威尔士州登记证，然后是您的驾照，还有一份安全检查证书。"),
            ("caller", "zh-cn", "com", "安全检查证书是什么？我需要在哪里做？"),
            ("agent", "zh-cn", "com", "这是一个车辆检查，确保您的车辆符合昆士兰州的安全标准。您可以在任何授权的检查站进行。Sunnybank附近有好几家。"),
            ("caller", "zh-cn", "com", "好的，谢谢你。我的手机号码是 0423 456 789。如果有什么问题可以打电话给我吗？"),
            ("agent", "zh-cn", "com", "当然可以。我已经记录了您的信息。您的参考号码是 TMR-2026-88721。祝您搬家顺利！"),
            ("caller", "zh-cn", "com", "非常感谢你的帮助！再见。"),
            ("agent", "zh-cn", "com", "再见张先生，祝您有美好的一天！"),
        ]
    },
    {
        "filename": "call_005_fraud_report.wav",
        "turns": [
            ("agent", "en", "com.au", "Transport and Main Roads fraud and security line, Ben O'Connor speaking. This call is being recorded for quality and security purposes."),
            ("caller", "en", "co.uk", "Hi, my name is Sandra Martinez. I need to report something urgent. I received a letter saying a vehicle has been registered in my name but I never registered any vehicle."),
            ("agent", "en", "com.au", "I understand this must be very concerning Sandra. Let me take down your details so we can investigate immediately. Can you confirm your full name, date of birth and current address?"),
            ("caller", "en", "co.uk", "Sandra Maria Martinez, born seventh of November nineteen seventy five. I live at nine hundred and twelve Pacific Highway, Mackay, four seven four zero."),
            ("agent", "en", "com.au", "Thank you. And the registration mentioned in the letter, do you have the registration number?"),
            ("caller", "en", "co.uk", "Yes, it says registration number Foxtrot Romeo Delta nine zero one. The vehicle is apparently a twenty twenty two Toyota Hilux. I have never owned a Hilux in my life."),
            ("agent", "en", "com.au", "I can see this registration in our system. It was registered on the twenty eighth of March this year using your licence number. Sandra, have you lost your licence or had your wallet stolen recently?"),
            ("caller", "en", "co.uk", "Oh my god, yes actually. My handbag was stolen at a shopping centre about six weeks ago. I reported it to the police but I didn't think to notify you as well."),
            ("agent", "en", "com.au", "That's likely how your identity was used. I'm going to flag this registration immediately, place a fraud alert on your account, and assign a case number. Do you have the police report number from the theft?"),
            ("caller", "en", "co.uk", "Yes it's Q P S dash two zero two six dash seven seven eight nine three."),
            ("agent", "en", "com.au", "Perfect. Your fraud case number is F R D dash zero four one four dash zero zero three. We'll investigate this thoroughly. In the meantime, I recommend you also contact your bank and credit reporting agencies. Would you like me to transfer you to our identity protection team?"),
            ("caller", "en", "co.uk", "Yes please. Thank you so much Ben, I was so worried about this."),
            ("agent", "en", "com.au", "You've done the right thing by calling us straight away. We take identity fraud very seriously. I'll transfer you now."),
        ]
    },
]


def tts_to_segment(text, lang, tld):
    tts = gTTS(text=text, lang=lang, tld=tld, slow=False)
    buf = io.BytesIO()
    tts.write_to_fp(buf)
    buf.seek(0)
    return AudioSegment.from_mp3(buf)


def build_call(call_def):
    audio = AudioSegment.silent(duration=500)
    for i, (role, lang, tld, text) in enumerate(call_def["turns"]):
        seg = tts_to_segment(text, lang, tld)
        if role == "caller":
            seg = seg._spawn(seg.raw_data, overrides={"frame_rate": int(seg.frame_rate * 0.95)}).set_frame_rate(seg.frame_rate)
        audio += seg + PAUSE
    audio += AudioSegment.silent(duration=500)
    audio = audio.set_channels(1).set_frame_rate(16000)
    out_path = os.path.join(OUT_DIR, call_def["filename"])
    audio.export(out_path, format="wav")
    duration = len(audio) / 1000.0
    size_kb = os.path.getsize(out_path) / 1024
    print(f"  {call_def['filename']}: {duration:.1f}s, {size_kb:.0f} KB")
    return out_path


if __name__ == "__main__":
    print(f"Generating {len(CALLS)} multi-speaker call recordings...")
    for call in CALLS:
        build_call(call)
    print(f"\nDone! Files in: {OUT_DIR}")
