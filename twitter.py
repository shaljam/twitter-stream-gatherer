import tweepy
import MyStreamListener

auth = tweepy.OAuthHandler('kIpQuw1LxS4DgatGEB0NwZfAm', 'a8r7hgRMNdqb2xxmhcWanzYg1FN6DgTpS4iANiA0lyos47fdQZ')
auth.set_access_token('2380084197-h1uU4twPIGIIrB3NqzQxW8yIhNswwKJwwsfsYzy', 'dKc5cZavlqBKqGmnA9kJM0WbWZfNuqFtwz8yPTyBvSgQX')

api = tweepy.API(auth)

myStreamListener = MyStreamListener.MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

myStream.filter(track=['مالیات', 'فقر', 'فقیر', 'قالیباف', 'خالیباف', 'جهانگیری', 'پلاسکو', 'شهرداری', 'آستان قدس',
                       'جمهوری', 'رای', 'انتخاب', 'رئیسی', 'میرسلیم', 'روحانی', 'هاشمی طبا', 'رشد اقتصادی',
                       'امام رضا', 'آستان قدس', 'انتخابات', 'موسوی', 'کروبی', 'حصر', 'کار', 'اشتغال', 'کرامت', 'وعده',
                       'خاتمی', 'رفسنجانی', 'شغل', 'برنامه', 'دولت', 'ایران', 'رئیس‌جمهور', 'رئیس جمهور',
                       'رهبر', 'انقلاب', 'رأی', 'ماليات', 'نتایج', 'اعتراض', 'تقلب', 'اول', 'دوم', 'سوم', 'دوازدهمین',
                       'دوازدهم', 'اصلاحطلب', 'اصلاح‌طلب', 'اصلاح طلب', 'اصولگرا', 'شورای شهر', 'لیست', 'تهران', 'اقوام',
                       'زنان', 'وزارت کشور', '۹۶', 'برجام', 'ارتباطات بانکی', 'توریسم', 'توریست', 'عادی سازی',
                       'روابط خارجی'], async=True)
