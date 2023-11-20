using System;
using System.Text;
using System.Runtime.InteropServices;
using System.Windows.Forms;
using System.Activities;

// https://learn.microsoft.com/en-us/archive/msdn-magazine/2002/november/cutting-edge-using-windows-hooks-to-enhance-messagebox-in-net

namespace MarkMpn.Sql4Cds.XTB
{
    static class YesYesToAllNoMessageBox
    {
        [DllImport("user32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        static extern bool SetWindowText(IntPtr hwnd, String lpString);

        [DllImport("user32.dll")]
        static extern IntPtr GetDlgItem(IntPtr hDlg, int nIDDlgItem);

        public static DialogResult Show(IWin32Window owner, string text, string title, MessageBoxIcon icon, out bool all)
        {
            var cbt = new LocalCbtHook();
            var handle = IntPtr.Zero;
            var alreadySetup = false;

            cbt.WindowCreated += (sender, e) =>
            {
                if (e.IsDialogWindow)
                {
                    alreadySetup = false;
                    handle = e.Handle;
                }
            };

            cbt.WindowActivated += (sender, e) =>
            {
                if (e.Handle == handle && !alreadySetup)
                {
                    alreadySetup = true;

                    // Map button text
                    SetWindowText(GetDlgItem(handle, (int)DialogResult.No), "Yes to &All");
                    SetWindowText(GetDlgItem(handle, (int)DialogResult.Cancel), DialogResult.No.ToString());
                }
            };

            cbt.WindowDestroyed += (sender, e) =>
            {
                if (e.Handle == handle)
                {
                    alreadySetup = false;
                    handle = IntPtr.Zero;
                }
            };

            cbt.Install();

            try
            {
                var result = MessageBox.Show(owner, text, title, MessageBoxButtons.YesNoCancel, icon, MessageBoxDefaultButton.Button3);

                switch (result)
                {
                    case DialogResult.Yes:
                        all = false;
                        return DialogResult.Yes;

                    case DialogResult.No:
                        all = true;
                        return DialogResult.Yes;

                    case DialogResult.Cancel:
                        all = false;
                        return DialogResult.No;

                    default:
                        all = false;
                        return result;
                }
            }
            finally
            {
                cbt.Uninstall();
            }
        }
    }
    
    // CBT hook actions
    enum CbtHookAction
    {
        HCBT_MOVESIZE = 0,
        HCBT_MINMAX = 1,
        HCBT_QS = 2,
        HCBT_CREATEWND = 3,
        HCBT_DESTROYWND = 4,
        HCBT_ACTIVATE = 5,
        HCBT_CLICKSKIPPED = 6,
        HCBT_KEYSKIPPED = 7,
        HCBT_SYSCOMMAND = 8,
        HCBT_SETFOCUS = 9
    }
    
    class CbtEventArgs : EventArgs
    {
        /// <summary>
        /// Win32 handle of the window
        /// </summary>
        public IntPtr Handle { get; set; }

        /// <summary>
        /// caption of the window
        /// </summary>
        public string Title { get; set; }

        /// <summary>
        /// class of the window
        /// </summary>
        public string ClassName { get; set; }

        /// <summary>
        /// whether it's a popup dialog
        /// </summary>
        public bool IsDialogWindow { get; set; }
    }

    /// <summary>
    /// Event delegate
    /// </summary>
    /// <param name="sender"></param>
    /// <param name="e"></param>
    delegate void CbtEventHandler(object sender, CbtEventArgs e);

    class LocalCbtHook : LocalWindowsHook
    {
        public event CbtEventHandler WindowCreated;
        public event CbtEventHandler WindowDestroyed;
        public event CbtEventHandler WindowActivated;
        
        private IntPtr _hwnd;
        private string _title;
        private string _class;
        private bool _isDialog;
        
        public LocalCbtHook() : base(HookType.WH_CBT)
        {
            HookInvoked += new HookEventHandler(CbtHookInvoked);
        }
        
        public LocalCbtHook(HookProc func) : base(HookType.WH_CBT, func)
        {
            HookInvoked += new HookEventHandler(CbtHookInvoked);
        }

        /// <summary>
        /// Handles the hook event
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void CbtHookInvoked(object sender, HookEventArgs e)
        {
            var code = (CbtHookAction)e.HookCode;
            var wParam = e.wParam;
            var lParam = e.lParam;

            // Handle hook events (only a few of available actions)
            switch (code)
            {
                case CbtHookAction.HCBT_CREATEWND:
                    HandleCreateWndEvent(wParam, lParam);
                    break;
                
                case CbtHookAction.HCBT_DESTROYWND:
                    HandleDestroyWndEvent(wParam, lParam);
                    break;
                
                case CbtHookAction.HCBT_ACTIVATE:
                    HandleActivateEvent(wParam, lParam);
                    break;
            }
            
            return;
        }

        /// <summary>
        /// Handle the CREATEWND hook event
        /// </summary>
        /// <param name="wParam"></param>
        /// <param name="lParam"></param>
        private void HandleCreateWndEvent(IntPtr wParam, IntPtr lParam)
        {
            // Cache some information
            UpdateWindowData(wParam);

            // raise event
            OnWindowCreated();
        }

        /// <summary>
        /// Handle the DESTROYWND hook event
        /// </summary>
        /// <param name="wParam"></param>
        /// <param name="lParam"></param>
        private void HandleDestroyWndEvent(IntPtr wParam, IntPtr lParam)
        {
            // Cache some information
            UpdateWindowData(wParam);

            // raise event
            OnWindowDestroyed();
        }

        /// <summary>
        /// Handle the ACTIVATE hook event
        /// </summary>
        /// <param name="wParam"></param>
        /// <param name="lParam"></param>
        private void HandleActivateEvent(IntPtr wParam, IntPtr lParam)
        {
            // Cache some information
            UpdateWindowData(wParam);

            // raise event
            OnWindowActivated();
        }

        /// <summary>
        /// Read and store some information about the window
        /// </summary>
        /// <param name="wParam"></param>
        private void UpdateWindowData(IntPtr wParam) 
        { 
            // Cache the window handle
            _hwnd = wParam; 

            // Cache the window's class name
            var className = new StringBuilder(40); 
            GetClassName(_hwnd, className, 40); 
            _class = className.ToString(); 

            // Cache the window's title bar
            var text = new StringBuilder(256); 
            GetWindowText(_hwnd, text, 256); 
            _title = text.ToString(); 

            // Cache the dialog flag
            _isDialog = _class == "#32770";
        }

        /// <summary>
        /// Helper functions that fire events by executing user code
        /// </summary>
        protected virtual void OnWindowCreated() 
        { 
            if (WindowCreated != null) 
            { 
                var e = new CbtEventArgs(); 
                PrepareEventData(e); 
                WindowCreated(this, e); 
            } 
        } 

        protected virtual void OnWindowDestroyed() 
        { 
            if (WindowDestroyed != null) 
            { 
                CbtEventArgs e = new CbtEventArgs(); 
                PrepareEventData(e); 
                WindowDestroyed(this, e); 
            } 
        } 

        protected virtual void OnWindowActivated() 
        { 
            if (WindowActivated != null) 
            { 
                CbtEventArgs e = new CbtEventArgs(); 
                PrepareEventData(e); 
                WindowActivated(this, e); 
            } 
        }

        /// <summary>
        /// Prepare the event data structure
        /// </summary>
        /// <param name="e"></param>
        private void PrepareEventData(CbtEventArgs e) 
        { 
            e.Handle = _hwnd; 
            e.Title = _title; 
            e.ClassName = _class; 
            e.IsDialogWindow = _isDialog; 
        } 

        [DllImport("user32.dll")] 
        private static extern int GetClassName(IntPtr hwnd, StringBuilder lpClassName, int nMaxCount); 
        
        [DllImport("user32.dll")] 
        private static extern int GetWindowText(IntPtr hwnd, StringBuilder lpString, int nMaxCount);
    }
        
    class HookEventArgs : EventArgs
    {
        /// <summary>
        /// Hook code
        /// </summary>
        public int HookCode { get; set; }

        /// <summary>
        /// WPARAM argument
        /// </summary>
        public IntPtr wParam { get; set; }

        /// <summary>
        /// LPARAM argument
        /// </summary>
        public IntPtr lParam { get; set; }
    } 
    
    // Hook Types
    enum HookType
    {
        WH_JOURNALRECORD = 0, 
        WH_JOURNALPLAYBACK = 1, 
        WH_KEYBOARD = 2, 
        WH_GETMESSAGE = 3, 
        WH_CALLWNDPROC = 4, 
        WH_CBT = 5, 
        WH_SYSMSGFILTER = 6, 
        WH_MOUSE = 7, 
        WH_HARDWARE = 8, 
        WH_DEBUG = 9, 
        WH_SHELL = 10, 
        WH_FOREGROUNDIDLE = 11, 
        WH_CALLWNDPROCRET = 12, 
        WH_KEYBOARD_LL = 13, 
        WH_MOUSE_LL = 14 
    } 
    
    class LocalWindowsHook 
    {   
        private IntPtr _hhook;
        private HookProc _filterFunc;
        private HookType _hookType;

        public event HookEventHandler HookInvoked; 
        
        protected void OnHookInvoked(HookEventArgs e) 
        { 
            if (HookInvoked != null) 
                HookInvoked(this, e); 
        }

        public LocalWindowsHook(HookType hook) 
        { 
            _hookType = hook; 
            _filterFunc = new HookProc(this.CoreHookProc); 
        } 

        public LocalWindowsHook(HookType hook, HookProc func) 
        { 
            _hookType = hook; 
            _filterFunc = func; 
        }

        /// <summary>
        /// Default filter function
        /// </summary>
        /// <param name="code"></param>
        /// <param name="wParam"></param>
        /// <param name="lParam"></param>
        /// <returns></returns>
        public int CoreHookProc(int code, IntPtr wParam, IntPtr lParam) 
        { 
            if (code < 0) 
                return CallNextHookEx(_hhook, code, wParam, lParam);

            // Let clients determine what to do
            var e = new HookEventArgs
            {
                HookCode = code,
                wParam = wParam,
                lParam = lParam
            };

            OnHookInvoked(e); 

            // Yield to the next hook in the chain
            return CallNextHookEx(_hhook, code, wParam, lParam); 
        }

        /// <summary>
        /// Install the hook
        /// </summary>
        public void Install() 
        { 
            _hhook = SetWindowsHookEx(_hookType, _filterFunc, IntPtr.Zero, AppDomain.GetCurrentThreadId()); 
        }

        /// <summary>
        /// Uninstall the hook
        /// </summary>
        public void Uninstall() 
        { 
            UnhookWindowsHookEx(_hhook); 
        }

        [DllImport("user32.dll")] 
        private static extern IntPtr SetWindowsHookEx(HookType code, HookProc func, IntPtr hInstance, int threadID); 
        
        [DllImport("user32.dll")] 
        private static extern int UnhookWindowsHookEx(IntPtr hhook); 
        
        [DllImport("user32.dll")] 
        private static extern int CallNextHookEx(IntPtr hhook, int code, IntPtr wParam, IntPtr lParam);
    }

    /// <summary>
    /// Filter function delegate
    /// </summary>
    /// <param name="code"></param>
    /// <param name="wParam"></param>
    /// <param name="lParam"></param>
    /// <returns></returns>
    delegate int HookProc(int code, IntPtr wParam, IntPtr lParam);

    /// <summary>
    /// Event delegate
    /// </summary>
    /// <param name="sender"></param>
    /// <param name="e"></param>
    delegate void HookEventHandler(object sender, HookEventArgs e);
}
